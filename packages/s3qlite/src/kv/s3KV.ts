import { S3 } from "@effect-aws/client-s3";
import type { S3Service } from "@effect-aws/client-s3";
import { Chunk, Effect, Option, Stream } from "effect";

import { ConflictError } from "./kv";
import type { KV } from "./kv";

const isMissingObjectError = (error: unknown): boolean =>
	Effect.runSync(
		Effect.sync(() => {
			if (typeof error !== "object") {
				return false;
			}

			const errorOption = Option.fromNullable(error);
			if (Option.isNone(errorOption)) {
				return false;
			}

			const currentError = errorOption.value as { Code?: unknown; name?: unknown };
			return (
				(typeof currentError.Code === "string" && currentError.Code === "NoSuchKey") ||
				(typeof currentError.name === "string" &&
					(currentError.name === "NoSuchKey" || currentError.name === "NotFound"))
			);
		}),
	);

const isNotModifiedError = (error: unknown): boolean =>
	Effect.runSync(
		Effect.sync(() => {
			if (typeof error !== "object") {
				return false;
			}

			const errorOption = Option.fromNullable(error);
			if (Option.isNone(errorOption)) {
				return false;
			}

			const currentError = errorOption.value as {
				Code?: unknown;
				name?: unknown;
				$metadata?: unknown;
			};
			const metadata =
				typeof currentError.$metadata === "object" &&
				Option.isSome(Option.fromNullable(currentError.$metadata))
					? Option.some(currentError.$metadata as { httpStatusCode?: number })
					: Option.none<{ httpStatusCode?: number }>();

			return (
				(typeof currentError.Code === "string" && currentError.Code === "NotModified") ||
				(typeof currentError.name === "string" && currentError.name === "NotModified") ||
				Option.exists(metadata, (currentMetadata) => currentMetadata.httpStatusCode === 304)
			);
		}),
	);

const getEtag = (result: { ETag?: string }, key: string): Effect.Effect<{ etag: string }> =>
	Option.fromNullable(result.ETag).pipe(
		Option.match({
			onNone: () => Effect.die(new Error(`Expected ETag for ${key}`)),
			onSome: (etag) => Effect.succeed({ etag }),
		}),
	);

const readBodyBytes = (body: unknown, key: string): Effect.Effect<Uint8Array> => {
	if (body instanceof Uint8Array) {
		return Effect.succeed(body);
	}

	if (typeof body === "object") {
		const bodyOption = Option.fromNullable(body);
		if (
			Option.isSome(bodyOption) &&
			"transformToByteArray" in bodyOption.value &&
			typeof bodyOption.value.transformToByteArray === "function"
		) {
			const currentBody = bodyOption.value as {
				transformToByteArray: () => Promise<Uint8Array>;
			};
			return Effect.promise(() => currentBody.transformToByteArray()).pipe(Effect.orDie);
		}
	}

	return Effect.die(new Error(`Expected readable body for ${key}`));
};

const readBodyStream = (body: unknown, key: string): Effect.Effect<Stream.Stream<Uint8Array>> => {
	if (body instanceof Uint8Array) {
		return Effect.succeed(Stream.fromIterable([body]));
	}

	if (body instanceof ReadableStream) {
		return Effect.succeed(
			Stream.fromReadableStream<Uint8Array, Error>(
				() => body as ReadableStream<Uint8Array>,
				() => new Error(`Failed to stream ${key}`),
			).pipe(Stream.orDie),
		);
	}

	if (typeof body === "object") {
		const bodyOption = Option.fromNullable(body);
		if (
			Option.isSome(bodyOption) &&
			"transformToWebStream" in bodyOption.value &&
			typeof bodyOption.value.transformToWebStream === "function"
		) {
			const currentBody = bodyOption.value as {
				transformToWebStream: () => ReadableStream<Uint8Array>;
			};
			return Effect.succeed(
				Stream.fromReadableStream<Uint8Array, Error>(
					() => currentBody.transformToWebStream(),
					() => new Error(`Failed to stream ${key}`),
				).pipe(Stream.orDie),
			);
		}
	}

	if (typeof body === "object") {
		const bodyOption = Option.fromNullable(body);
		if (
			Option.isSome(bodyOption) &&
			"transformToByteArray" in bodyOption.value &&
			typeof bodyOption.value.transformToByteArray === "function"
		) {
			return readBodyBytes(bodyOption.value, key).pipe(
				Effect.map((bytes) => Stream.fromIterable([bytes])),
			);
		}
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

export const makeS3KV = (bucket: string): Effect.Effect<KV, never, S3Service> =>
	Effect.gen(function* () {
		const s3 = yield* S3;

		return {
			get: (key) =>
				Effect.gen(function* () {
					yield* Effect.logDebug(`s3KV.get(${key})`);

					const response = yield* Effect.either(
						s3.getObject({ Bucket: bucket, Key: key }),
					);
					if (response._tag === "Left") {
						if (isMissingObjectError(response.left)) {
							yield* Effect.logDebug(`s3KV.get(${key}) => None`);
							return Option.none();
						}

						return yield* Effect.die(
							response.left instanceof Error
								? response.left
								: new Error(`Failed to read ${key}: ${String(response.left)}`),
						);
					}

					const { etag } = yield* getEtag(response.right, key);
					const value = yield* readBodyBytes(response.right.Body, key);
					yield* Effect.logDebug(`s3KV.get(${key}) => Some(${etag})`);
					return Option.some({ etag, value });
				}),
			getIfChanged: (key, etag) =>
				Effect.gen(function* () {
					yield* Effect.logDebug(`s3KV.getIfChanged(${key}, ${etag})`);

					const response = yield* Effect.either(
						s3.getObject({ Bucket: bucket, IfNoneMatch: etag, Key: key }),
					);
					if (response._tag === "Left") {
						if (
							isMissingObjectError(response.left) ||
							isNotModifiedError(response.left)
						) {
							yield* Effect.logDebug(`s3KV.getIfChanged(${key}) => None`);
							return Option.none();
						}

						return yield* Effect.die(
							response.left instanceof Error
								? response.left
								: new Error(`Failed to read ${key}: ${String(response.left)}`),
						);
					}

					const { etag: currentEtag } = yield* getEtag(response.right, key);
					const value = yield* readBodyBytes(response.right.Body, key);
					yield* Effect.logDebug(`s3KV.getIfChanged(${key}) => Some(${currentEtag})`);
					return Option.some({ etag: currentEtag, value });
				}),
			exists: (key) =>
				Effect.gen(function* () {
					yield* Effect.logDebug(`s3KV.exists(${key})`);

					const response = yield* Effect.either(
						s3.headObject({ Bucket: bucket, Key: key }),
					);
					if (response._tag === "Left") {
						if (isMissingObjectError(response.left)) {
							yield* Effect.logDebug(`s3KV.exists(${key}) => false`);
							return false;
						}

						return yield* Effect.die(
							response.left instanceof Error
								? response.left
								: new Error(`Failed to stat ${key}: ${String(response.left)}`),
						);
					}

					yield* Effect.logDebug(`s3KV.exists(${key}) => true`);
					return true;
				}),
			set: (key, value) =>
				Effect.logDebug(`s3KV.set(${key}, ${value.length} bytes)`).pipe(
					Effect.flatMap(() => s3.putObject({ Body: value, Bucket: bucket, Key: key })),
					Effect.orDieWith((error) =>
						error instanceof Error
							? error
							: new Error(`Failed to write ${key}: ${String(error)}`),
					),
					Effect.flatMap((result) => getEtag(result, key)),
					Effect.tap((result) => Effect.logDebug(`s3KV.set(${key}) => ${result.etag}`)),
				),
			cas: (key, value, etag) =>
				Effect.logDebug(`s3KV.cas(${key}, ${value.length} bytes, ${etag})`).pipe(
					Effect.flatMap(() =>
						s3.putObject({ Body: value, Bucket: bucket, IfMatch: etag, Key: key }),
					),
					Effect.catchAll((error) => {
						const currentError =
							typeof error === "object" && Option.isSome(Option.fromNullable(error))
								? Option.some(
										error as {
											Code?: unknown;
											name?: unknown;
											$metadata?: unknown;
										},
									)
								: Option.none<{
										Code?: unknown;
										name?: unknown;
										$metadata?: unknown;
									}>();
						const metadata =
							Option.isSome(currentError) &&
							typeof currentError.value.$metadata === "object" &&
							Option.isSome(Option.fromNullable(currentError.value.$metadata))
								? Option.some(
										currentError.value.$metadata as { httpStatusCode?: number },
									)
								: Option.none<{ httpStatusCode?: number }>();

						return Option.isSome(currentError) &&
							((typeof currentError.value.Code === "string" &&
								currentError.value.Code === "PreconditionFailed") ||
								(typeof currentError.value.name === "string" &&
									currentError.value.name === "PreconditionFailed")) &&
							Option.exists(
								metadata,
								(currentMetadata) => currentMetadata.httpStatusCode === 412,
							)
							? Effect.fail(new ConflictError({ key }))
							: Effect.die(
									error instanceof Error
										? error
										: new Error(`Failed to write ${key}: ${String(error)}`),
								);
					}),
					Effect.flatMap((result) => getEtag(result, key)),
					Effect.tap((result) => Effect.logDebug(`s3KV.cas(${key}) => ${result.etag}`)),
				),
			delete: (key) =>
				Effect.logDebug(`s3KV.delete(${key})`).pipe(
					Effect.flatMap(() => s3.deleteObject({ Bucket: bucket, Key: key })),
					Effect.asVoid,
					Effect.orDieWith((error) =>
						error instanceof Error
							? error
							: new Error(`Failed to delete ${key}: ${String(error)}`),
					),
					Effect.tap(() => Effect.logDebug(`s3KV.delete(${key}) => void`)),
				),
			readStream: (key) =>
				Effect.gen(function* () {
					yield* Effect.logDebug(`s3KV.readStream(${key})`);

					const response = yield* Effect.either(
						s3.getObject({ Bucket: bucket, Key: key }),
					);
					if (response._tag === "Left") {
						if (isMissingObjectError(response.left)) {
							yield* Effect.logDebug(`s3KV.readStream(${key}) => None`);
							return Option.none();
						}

						return yield* Effect.die(
							response.left instanceof Error
								? response.left
								: new Error(`Failed to stream ${key}: ${String(response.left)}`),
						);
					}

					const stream = yield* readBodyStream(response.right.Body, key);
					yield* Effect.logDebug(`s3KV.readStream(${key}) => Some(stream)`);
					return Option.some(stream);
				}),
			writeStream: (key, content) =>
				Effect.gen(function* () {
					yield* Effect.logDebug(`s3KV.writeStream(${key}, stream)`);

					const multipartUpload = yield* s3
						.createMultipartUpload({ Bucket: bucket, Key: key })
						.pipe(
							Effect.orDieWith((error) =>
								error instanceof Error
									? error
									: new Error(`Failed to start upload ${key}: ${String(error)}`),
							),
						);
					const uploadIdOption = Option.fromNullable(multipartUpload.UploadId);
					if (Option.isNone(uploadIdOption)) {
						return yield* Effect.die(new Error(`Expected UploadId for ${key}`));
					}

					const uploadId = uploadIdOption.value;
					const uploadParts = Effect.scoped(
						Effect.gen(function* () {
							const pull = yield* Stream.toPull(content);
							const completedParts: { ETag: string; PartNumber: number }[] = [];
							let bufferedChunks: Uint8Array[] = [];
							let bufferedLength = 0;
							let partNumber = 1;

							const uploadBufferedPart = (force: boolean): Effect.Effect<void> => {
								if (
									bufferedLength === 0 ||
									(!force && bufferedLength < minimumMultipartPartSize)
								) {
									return Effect.void;
								}

								const body = combineChunks(bufferedChunks, bufferedLength);
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
										Effect.orDieWith((error) =>
											error instanceof Error
												? error
												: new Error(
														`Failed to upload ${key} part ${partNumber}: ${String(error)}`,
													),
										),
										Effect.flatMap((result) =>
											Option.fromNullable(result.ETag).pipe(
												Option.match({
													onNone: () =>
														Effect.die(
															new Error(
																`Expected ETag for ${key} part ${partNumber}`,
															),
														),
													onSome: (etag) =>
														Effect.sync(() => {
															completedParts.push({
																ETag: etag,
																PartNumber: partNumber,
															});
															partNumber += 1;
														}),
												}),
											),
										),
									);
							};

							while (true) {
								const nextChunk = yield* Effect.either(pull);

								if (nextChunk._tag === "Left") {
									return yield* uploadBufferedPart(true).pipe(
										Effect.as(completedParts),
									);
								}

								for (const chunk of Chunk.toReadonlyArray(nextChunk.right)) {
									bufferedChunks.push(chunk);
									bufferedLength += chunk.length;
									if (bufferedLength >= minimumMultipartPartSize) {
										yield* uploadBufferedPart(false);
									}
								}
							}
						}),
					).pipe(
						Effect.catchAllCause((cause) =>
							s3
								.abortMultipartUpload({
									Bucket: bucket,
									Key: key,
									UploadId: uploadId,
								})
								.pipe(
									Effect.orDie,
									Effect.catchAll(() => Effect.void),
									Effect.zipRight(Effect.failCause(cause)),
								),
						),
					);

					const completedParts = yield* uploadParts;

					if (completedParts.length === 0) {
						const emptyResult = yield* s3
							.putObject({ Body: new Uint8Array(), Bucket: bucket, Key: key })
							.pipe(
								Effect.orDieWith((error) =>
									error instanceof Error
										? error
										: new Error(`Failed to upload ${key}: ${String(error)}`),
								),
							);

						const emptyEtagResult = yield* getEtag(emptyResult, key);
						yield* Effect.logDebug(
							`s3KV.writeStream(${key}) => ${emptyEtagResult.etag}`,
						);
						return emptyEtagResult;
					}

					const result = yield* s3
						.completeMultipartUpload({
							Bucket: bucket,
							Key: key,
							MultipartUpload: { Parts: completedParts },
							UploadId: uploadId,
						})
						.pipe(
							Effect.orDieWith((error) =>
								error instanceof Error
									? error
									: new Error(`Failed to upload ${key}: ${String(error)}`),
							),
						);

					const finalResult = yield* getEtag(result, key);
					yield* Effect.logDebug(`s3KV.writeStream(${key}) => ${finalResult.etag}`);
					return finalResult;
				}),
		} satisfies KV;
	});