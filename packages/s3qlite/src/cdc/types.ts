// oxlint-disable-next-line local-rules/no-null-undefined-option -- SQLite CDC rows must preserve SQL NULL values.
export type CDCValue = number | string | Uint8Array | null;

export const CDCChangeType = {
	Delete: -1,
	Update: 0,
	Insert: 1,
	// Commit: 2,
} as const;

export type CDCChangeType = (typeof CDCChangeType)[keyof typeof CDCChangeType];

type CDCBase = {
	changeId: number;
	changeTime: number;
	changeTxnId: number;
};

export type CDCRow =
	| (CDCBase & {
			changeType: typeof CDCChangeType.Insert;
			tableName: string;
			id: CDCValue;
			// oxlint-disable-next-line local-rules/no-null-undefined-option -- Insert CDC rows have no previous record image.
			before: Uint8Array | null;
			after: Uint8Array;
			// oxlint-disable-next-line local-rules/no-null-undefined-option -- Insert CDC rows have no updates mask.
			updates: Uint8Array | null;
	  })
	| (CDCBase & {
			changeType: typeof CDCChangeType.Update;
			tableName: string;
			id: CDCValue;
			before: Uint8Array;
			after: Uint8Array;
			updates: Uint8Array;
	  })
	| (CDCBase & {
			changeType: typeof CDCChangeType.Delete;
			tableName: string;
			id: CDCValue;
			before: Uint8Array;
			// oxlint-disable-next-line local-rules/no-null-undefined-option -- Delete CDC rows have no post-delete record image.
			after: Uint8Array | null;
			// oxlint-disable-next-line local-rules/no-null-undefined-option -- Delete CDC rows have no updates mask.
			updates: Uint8Array | null;
	  });