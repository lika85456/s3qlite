import protobuf from "protobufjs";

import type { CDCRow, CDCValue } from "./types";

const schema = protobuf.parse(`
syntax = "proto3";

message CdcValue {
	oneof kind {
		bool null_value = 1;
		double number_value = 2;
		string string_value = 3;
		bytes bytes_value = 4;
	}
}

message CdcRow {
	sint64 change_id = 1;
	sint64 change_time = 2;
	sint64 change_txn_id = 3;
	sint32 change_type = 4;
	string table_name = 5;
	CdcValue id = 6;
	bytes before = 7;
	bytes after = 8;
	bytes updates = 9;
}

message CdcBatch {
	repeated CdcRow changes = 1;
}
`).root;

const batchType = schema.lookupType("CdcBatch") as protobuf.Type;

type ValueMessage =
	| { kind: "nullValue"; nullValue: boolean }
	| { kind: "numberValue"; numberValue: number }
	| { kind: "stringValue"; stringValue: string }
	| { kind: "bytesValue"; bytesValue: Uint8Array };

type RowMessage = {
	changeId: number | bigint;
	changeTime: number | bigint;
	changeTxnId: number | bigint;
	changeType: number;
	tableName?: string;
	id?: ValueMessage;
	before?: Uint8Array;
	after?: Uint8Array;
	updates?: Uint8Array;
};

type BatchMessage = {
	changes: readonly RowMessage[];
};

const encodeValue = (value: CDCValue): ValueMessage => {
	// oxlint-disable-next-line local-rules/no-null-undefined-option -- CDC payloads must preserve SQL NULL values.
	if (value === null) {
		return { kind: "nullValue", nullValue: true };
	}
	if (typeof value === "number") {
		return { kind: "numberValue", numberValue: value };
	}
	if (typeof value === "string") {
		return { kind: "stringValue", stringValue: value };
	}
	return { kind: "bytesValue", bytesValue: value };
};

// oxlint-disable-next-line local-rules/no-null-undefined-option -- protobufjs omits absent optional fields as undefined on decode.
const decodeValue = (value: ValueMessage | undefined): CDCValue => {
	if (!value) {
		// oxlint-disable-next-line local-rules/no-null-undefined-option -- missing protobuf field maps back to SQL NULL.
		return null;
	}
	switch (value.kind) {
		case "nullValue":
			// oxlint-disable-next-line local-rules/no-null-undefined-option -- protobuf null arm maps to SQL NULL.
			return null;
		case "numberValue":
			return value.numberValue;
		case "stringValue":
			return value.stringValue;
		case "bytesValue":
			return Uint8Array.from(value.bytesValue);
	}
};

const encodeRow = (row: CDCRow): RowMessage => ({
	changeId: row.changeId,
	changeTime: row.changeTime,
	changeTxnId: row.changeTxnId,
	changeType: row.changeType,
	// oxlint-disable-next-line local-rules/no-null-undefined-option -- protobufjs optional fields are omitted with undefined during encoding.
	...(row.tableName === null ? {} : { tableName: row.tableName }),
	// oxlint-disable-next-line local-rules/no-null-undefined-option -- protobufjs optional fields are omitted with undefined during encoding.
	...(row.id === null ? {} : { id: encodeValue(row.id) }),
	// oxlint-disable-next-line local-rules/no-null-undefined-option -- protobufjs optional fields are omitted with undefined during encoding.
	...(row.before === null ? {} : { before: row.before }),
	// oxlint-disable-next-line local-rules/no-null-undefined-option -- protobufjs optional fields are omitted with undefined during encoding.
	...(row.after === null ? {} : { after: row.after }),
	// oxlint-disable-next-line local-rules/no-null-undefined-option -- protobufjs optional fields are omitted with undefined during encoding.
	...(row.updates === null ? {} : { updates: row.updates }),
});

const decodeRow = (row: RowMessage): CDCRow => {
	const changeType = row.changeType as CDCRow["changeType"];
	if (changeType === -1) {
		return {
			changeId: Number(row.changeId),
			changeTime: Number(row.changeTime),
			changeTxnId: Number(row.changeTxnId),
			changeType,
			tableName: row.tableName || "",
			id: decodeValue(row.id),
			before: row.before ? Uint8Array.from(row.before) : new Uint8Array(),
			// oxlint-disable-next-line local-rules/no-null-undefined-option -- Delete CDC rows have no post-delete image.
			after: null,
			// oxlint-disable-next-line local-rules/no-null-undefined-option -- Delete CDC rows have no updates mask.
			updates: null,
		};
	}
	if (changeType === 0) {
		return {
			changeId: Number(row.changeId),
			changeTime: Number(row.changeTime),
			changeTxnId: Number(row.changeTxnId),
			changeType,
			tableName: row.tableName || "",
			id: decodeValue(row.id),
			before: row.before ? Uint8Array.from(row.before) : new Uint8Array(),
			after: row.after ? Uint8Array.from(row.after) : new Uint8Array(),
			updates: row.updates ? Uint8Array.from(row.updates) : new Uint8Array(),
		};
	}
	if (changeType === 1) {
		return {
			changeId: Number(row.changeId),
			changeTime: Number(row.changeTime),
			changeTxnId: Number(row.changeTxnId),
			changeType,
			tableName: row.tableName || "",
			id: decodeValue(row.id),
			// oxlint-disable-next-line local-rules/no-null-undefined-option -- Insert CDC rows have no previous image.
			before: null,
			after: row.after ? Uint8Array.from(row.after) : new Uint8Array(),
			// oxlint-disable-next-line local-rules/no-null-undefined-option -- Insert CDC rows have no updates mask.
			updates: null,
		};
	}
	throw new Error(`Unsupported CDC change type ${String(row.changeType)}`);
};

export const serializeCDC = (changes: readonly CDCRow[]): Uint8Array =>
	batchType.encode({ changes: changes.map(encodeRow) } as BatchMessage).finish();

export const deserializeCDC = (bytes: Uint8Array): readonly CDCRow[] =>
	(batchType.decode(bytes) as unknown as BatchMessage).changes.map(decodeRow);