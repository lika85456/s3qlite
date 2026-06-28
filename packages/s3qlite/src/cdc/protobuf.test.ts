import { describe, expect, it } from "@effect/vitest";

import { deserializeCDC, serializeCDC } from "./protobuf";
import { CDCChangeType } from "./types";
import type { CDCRow } from "./types";

describe("cdc protobuf", () => {
	it("round trips rows", () => {
		const input: readonly CDCRow[] = [
			{
				changeId: 1,
				changeTime: 2,
				changeTxnId: 3,
				changeType: CDCChangeType.Insert,
				tableName: "users",
				id: "u1",
				// oxlint-disable-next-line local-rules/no-null-undefined-option -- the round-trip test must cover nullable insert fields.
				before: null,
				after: Uint8Array.from([1, 2, 3]),
				// oxlint-disable-next-line local-rules/no-null-undefined-option -- the round-trip test must cover nullable insert fields.
				updates: null,
			},
			{
				changeId: 4,
				changeTime: 5,
				changeTxnId: 6,
				changeType: CDCChangeType.Update,
				tableName: "posts",
				id: Uint8Array.from([4, 5]),
				before: Uint8Array.from([6, 7]),
				after: Uint8Array.from([8, 9]),
				updates: Uint8Array.from([10]),
			},
		];

		expect(deserializeCDC(serializeCDC(input))).toEqual(input);
	});
});