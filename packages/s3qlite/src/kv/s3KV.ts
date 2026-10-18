import { S3 } from "@effect-aws/client-s3";
import { Data, Effect, Option, Predicate, Stream } from "effect";

import { ConflictError } from "./kv";
import type { KV, ListedObject } from "./kv";

export class BucketDoesNotExistError extends Data.TaggedError("BucketDoesNotExistError")<{
	readonly bucket: string;
	readonly message: string;
}> {}

type S3ErrorShape = {
	readonly _tag?: unknown;
	readonly Code?: unknown;
	readonly name?: unknown;
	readonly $metadata?: { readonly httpStatusCode?: unknown };
};

type S3ErrorKind = { readonly statuses: readonly number[]; readonly names: readonly string[] };

const notModified: S3ErrorKind = { statuses: [304], names: ["NotModified"] };
const missingObject: S3ErrorKind = { statuses: [404], names: ["NoSuchKey", "NotFound"] };
const missingBucket: S3ErrorKind = { statuses: [404], names: ["NoSuchBucket", "NotFound"] };
/** Some S3 implementations answer a concurrent conditional write with 409 instead of 412. */
const conditionalConflict: S3ErrorKind = {
	statuses: [409, 412],
	names: ["PreconditionFailed", "ConditionalRequestConflict"],
};

/**
 * `@effect-aws` keeps only the error names it declares for each command and turns every other
 * service exception into `SdkError`, so the HTTP status is checked as well as the name.
 */
const isS3Error = (error: unknown, kind: S3ErrorKind): boolean => {
	if (typeof error !== "object") {
		return false;
	}

	return Option.exists(Option.fromNullishOr(error as S3ErrorShape), (current) => {
		const status = current.$metadata?.httpStatusCode;
		const statusMatches = typeof status === "number" && kind.statuses.includes(status);
		return (
			statusMatches ||
			[current._tag, current.Code, current.name].some(
				(value) => typeof value === "string" && kind.names.includes(value),
			)
		);
	});
};

const toDefect = (action: string, key: string, error: unknown): Error =>
	error instanceof Error
		? error
		: new Error(
				`Failed to ${action} ${key}: ${typeof error === "object" ? JSON.stringify(error) : String(error)}`,
			);

const getEtag = (result: { ETag?: string }, key: string): Effect.Effect<{ etag: string }> =>
	Option.fromNullishOr(result.ETag).pipe(
		Option.match({
			onNone: () => Effect.die(new Error(`Expected ETag for ${key}`)),
			onSome: (etag) => Effect.succeed({ etag }),
		}),
	);

type ByteArrayBody = { transformToByteArray: () => Promise<Uint8Array> };
type WebStreamBody = { transformToWebStream: () => ReadableStream<Uint8Array> };

const readBodyBytes = (body: unknown, key: string): Effect.Effect<Uint8Array> => {
	if (body instanceof Uint8Array) {
		return Effect.succeed(body);
	}

	if (Predicate.hasProperty(body, "transformToByteArray")) {
		return Effect.promise(() => (body as ByteArrayBody).transformToByteArray());
	}

	return Effect.die(new Error(`Expected readable body for ${key}`));
};

const readBodyStream = (body: unknown, key: string): Effect.Effect<Stream.Stream<Uint8Array>> => {
	if (body instanceof Uint8Array) {
		return Effect.succeed(Stream.make(body));
	}

	const webStream =
		body instanceof ReadableStream
			? Option.some(() => body as ReadableStream<Uint8Array>)
			: Predicate.hasProperty(body, "transformToWebStream")
				? Option.some(() => (body as WebStreamBody).transformToWebStream())
				: Option.none<() => ReadableStream<Uint8Array>>();

	if (Option.isSome(webStream)) {
		return Effect.succeed(
			Stream.fromReadableStream({
				evaluate: webStream.value,
				onError: () => new Error(`Failed to stream ${key}`),
			}).pipe(Stream.orDie),
		);
	}

	if (Predicate.hasProperty(body, "transformToByteArray")) {
		return readBodyBytes(body, key).pipe(Effect.map((bytes) => Stream.make(bytes)));
	}

	return Effect.die(new Error(`Expected streamable body for ${key}`));
};

const minimumMultipartPartSize = 5 * 1024 * 1024;

const combineChunks = (chunks: readonly Uint8Array[], totalLength: number): Uint8Array => {
	const combined = new Uint8Array(totalLength);
	let offset = 0;

	for (const chunk of chunks) {
		combined.set(chunk, offset);
		offset += chunk.length;
	}

	return combined;
};

export const makeS3KV = (bucket: string) =>
	Effect.gen(function* () {
		const s3 = yield* S3;

		yield* s3.headBucket({ Bucket: bucket }).pipe(
			Effect.catch((error) =>
				isS3Error(error, missingBucket)
					? Effect.fail(
							new BucketDoesNotExistError({
								bucket,
								message: `Bucket "${bucket}" does not exist`,
							}),
						)
					: Effect.die(toDefect("access bucket", bucket, error)),
			),
		);

		const getObject = (key: string, ifNoneMatch: Option.Option<string>) =>
			s3
				.getObject({
					Bucket: bucket,
					Key: key,
					...Option.match(ifNoneMatch, {
						onNone: () => ({}),
						onSome: (etag) => ({ IfNoneMatch: etag }),
					}),
				})
				.pipe(
					Effect.map(Option.some),
					Effect.catch((error) =>
						isS3Error(error, missingObject) ||
						(Option.isSome(ifNoneMatch) && isS3Error(error, notModified))
							? Effect.succeed(Option.none())
							: Effect.die(toDefect("read", key, error)),
					),
				);

		const readValue = (key: string, ifNoneMatch: Option.Option<string>) =>
			Effect.gen(function* () {
				const response = yield* getObject(key, ifNoneMatch);
				if (Option.isNone(response)) {
					return Option.none<{ value: Uint8Array; etag: string }>();
				}

				const { etag } = yield* getEtag(response.value, key);
				const value = yield* readBodyBytes(response.value.Body, key);
				return Option.some({ etag, value });
			});

		const conditionalPut = (
			key: string,
			value: Uint8Array,
			condition: { readonly IfMatch: string } | { readonly IfNoneMatch: "*" },
		) =>
			s3.putObject({ Body: value, Bucket: bucket, Key: key, ...condition }).pipe(
				Effect.catch((error) =>
					// `If-Match` on a missing key answers 404; the expected version is gone either way.
					isS3Error(error, conditionalConflict) ||
					("IfMatch" in condition && isS3Error(error, missingObject))
						? Effect.fail(new ConflictError({ key }))
						: Effect.die(toDefect("write", key, error)),
				),
				Effect.flatMap((result) => getEtag(result, key)),
			);

		return {
			get: (key) =>
				readValue(key, Option.none()).pipe(
					Effect.tap((result) =>
						Effect.logDebug(`s3KV.get(${key}) => ${Option.isSome(result)}`),
					),
				),
			getIfChanged: (key, etag) =>
				readValue(key, Option.some(etag)).pipe(
					Effect.tap((result) =>
						Effect.logDebug(
							`s3KV.getIfChanged(${key}, ${etag}) => ${Option.isSome(result)}`,
						),
					),
				),
			exists: (key) =>
				s3.headObject({ Bucket: bucket, Key: key }).pipe(
					Effect.as(true),
					Effect.catch((error) =>
						isS3Error(error, missingObject)
							? Effect.succeed(false)
							: Effect.die(toDefect("stat", key, error)),
					),
					Effect.tap((result) => Effect.logDebug(`s3KV.exists(${key}) => ${result}`)),
				),
			set: (key, value) =>
				s3.putObject({ Body: value, Bucket: bucket, Key: key }).pipe(
					Effect.catch((error) => Effect.die(toDefect("write", key, error))),
					Effect.flatMap((result) => getEtag(result, key)),
					Effect.tap((result) => Effect.logDebug(`s3KV.set(${key}) => ${result.etag}`)),
				),
			cas: (key, value, etag) =>
				conditionalPut(key, value, { IfMatch: etag }).pipe(
					Effect.tap((result) => Effect.logDebug(`s3KV.cas(${key}) => ${result.etag}`)),
				),
			create: (key, value) =>
				conditionalPut(key, value, { IfNoneMatch: "*" }).pipe(
					Effect.tap((result) =>
						Effect.logDebug(`s3KV.create(${key}) => ${result.etag}`),
					),
				),
			delete: (key) =>
				s3.deleteObject({ Bucket: bucket, Key: key }).pipe(
					Effect.asVoid,
					Effect.catch((error) => Effect.die(toDefect("delete", key, error))),
					Effect.tap(() => Effect.logDebug(`s3KV.delete(${key})`)),
				),
			list: (prefix) =>
				Effect.gen(function* () {
					const objects: ListedObject[] = [];
					let token = Option.none<string>();
					do {
						const page = yield* s3
							.listObjectsV2({
								Bucket: bucket,
								Prefix: prefix,
								...Option.match(token, {
									onNone: () => ({}),
									onSome: (ContinuationToken) => ({ ContinuationToken }),
								}),
							})
							.pipe(
								Effect.catch((error) =>
									Effect.die(toDefect("list", prefix, error)),
								),
							);
						for (const object of page.Contents ?? []) {
							if (object.Key) {
								objects.push({
									key: object.Key,
									lastModified: object.LastModified?.getTime() ?? 0,
								});
							}
						}
						token = page.IsTruncated
							? Option.fromNullishOr(page.NextContinuationToken)
							: Option.none();
					} while (Option.isSome(token));
					return objects;
				}),
			readStream: (key) =>
				Effect.gen(function* () {
					const response = yield* getObject(key, Option.none());
					if (Option.isNone(response)) {
						return Option.none<Stream.Stream<Uint8Array>>();
					}

					return Option.some(yield* readBodyStream(response.value.Body, key));
				}),
			writeStream: (key, content) =>
				Effect.gen(function* () {
					const multipartUpload = yield* s3
						.createMultipartUpload({ Bucket: bucket, Key: key })
						.pipe(
							Effect.catch((error) =>
								Effect.die(toDefect("start upload", key, error)),
							),
						);
					const uploadIdOption = Option.fromNullishOr(multipartUpload.UploadId);
					if (Option.isNone(uploadIdOption)) {
						return yield* Effect.die(new Error(`Expected UploadId for ${key}`));
					}

					const uploadId = uploadIdOption.value;
					const completedParts: { ETag: string; PartNumber: number }[] = [];
					let bufferedChunks: Uint8Array[] = [];
					let bufferedLength = 0;

					const uploadBufferedPart = (force: boolean): Effect.Effect<void> => {
						if (
							bufferedLength === 0 ||
							(!force && bufferedLength < minimumMultipartPartSize)
						) {
							return Effect.void;
						}

						const body = combineChunks(bufferedChunks, bufferedLength);
						const partNumber = completedParts.length + 1;
						bufferedChunks = [];
						bufferedLength = 0;

						return s3
							.uploadPart({
								Body: body,
								Bucket: bucket,
								ContentLength: body.length,
								Key: key,
								PartNumber: partNumber,
								UploadId: uploadId,
							})
							.pipe(
								Effect.catch((error) =>
									Effect.die(
										toDefect(`upload part ${partNumber} of`, key, error),
									),
								),
								Effect.flatMap((result) =>
									getEtag(result, `${key} part ${partNumber}`),
								),
								Effect.flatMap(({ etag }) =>
									Effect.sync(() =>
										completedParts.push({ ETag: etag, PartNumber: partNumber }),
									),
								),
								Effect.asVoid,
							);
					};

					yield* Stream.runForEach(content, (chunk) => {
						bufferedChunks.push(chunk);
						bufferedLength += chunk.length;
						return uploadBufferedPart(false);
					}).pipe(
						Effect.andThen(Effect.suspend(() => uploadBufferedPart(true))),
						Effect.onError(() =>
							s3
								.abortMultipartUpload({
									Bucket: bucket,
									Key: key,
									UploadId: uploadId,
								})
								.pipe(Effect.ignore),
						),
					);

					if (completedParts.length === 0) {
						yield* s3
							.abortMultipartUpload({ Bucket: bucket, Key: key, UploadId: uploadId })
							.pipe(Effect.ignore);
						const emptyResult = yield* s3
							.putObject({ Body: new Uint8Array(), Bucket: bucket, Key: key })
							.pipe(
								Effect.catch((error) => Effect.die(toDefect("upload", key, error))),
							);
						return yield* getEtag(emptyResult, key);
					}

					const result = yield* s3
						.completeMultipartUpload({
							Bucket: bucket,
							Key: key,
							MultipartUpload: { Parts: completedParts },
							UploadId: uploadId,
						})
						.pipe(Effect.catch((error) => Effect.die(toDefect("upload", key, error))));

					const finalResult = yield* getEtag(result, key);
					yield* Effect.logDebug(`s3KV.writeStream(${key}) => ${finalResult.etag}`);
					return finalResult;
				}),
		} satisfies KV;
	});