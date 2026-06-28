import { S3 } from "@effect-aws/client-s3";
import { expect } from "@effect/vitest";
import { Effect, Option, Stream } from "effect";

import { makeS3KV } from "../src/kv/s3KV";

import { Rustfs, rustfs } from "./utils/rustfs";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

rustfs("s3 kv", (it) => {
	it.effect("writes streamed snapshot bytes to real s3 and reads them back", () =>
		Effect.gen(function* () {
			const s3 = yield* S3;
			const { makeBucketName } = yield* Rustfs;
			const bucket = makeBucketName("s3kv");
			const kv = yield* makeS3KV(bucket);
			const expectedText = "snapshot-bytes-from-stream";
			const key = `${crypto.randomUUID()}.snapshot`;

			yield* s3.createBucket({ Bucket: bucket });
			yield* kv.writeStream(
				key,
				Stream.fromIterable([
					textEncoder.encode("snapshot-"),
					textEncoder.encode("bytes-"),
					textEncoder.encode("from-stream"),
				]),
			);

			const storedValue = yield* kv.get(key).pipe(
				Effect.flatMap(
					Option.match({
						onNone: () => Effect.fail(new Error(`Missing uploaded object ${key}`)),
						onSome: ({ value }) => Effect.succeed(value),
					}),
				),
			);
			expect(textDecoder.decode(storedValue)).toBe(expectedText);

			const storedStream = yield* kv.readStream(key).pipe(
				Effect.flatMap(
					Option.match({
						onNone: () => Effect.fail(new Error(`Missing uploaded stream ${key}`)),
						onSome: Effect.succeed,
					}),
				),
			);
			const readBack = Array.from(yield* Stream.runCollect(storedStream));
			const totalLength = readBack.reduce((length, chunk) => length + chunk.length, 0);
			const bytes = new Uint8Array(totalLength);
			let offset = 0;

			for (const chunk of readBack) {
				bytes.set(chunk, offset);
				offset += chunk.length;
			}

			expect(textDecoder.decode(bytes)).toBe(expectedText);
		}),
	);
});