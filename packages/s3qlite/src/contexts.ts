import type { Database } from "@tursodatabase/database";
import { Context } from "effect";
import type { Effect } from "effect";

import type { ConnectionOptions } from "./types";

export type ConnectionConfigValue = {
	readonly bucket: string;
	readonly dbName: string;
	readonly livePath: string;
	readonly localDirectory: string;
	readonly localHeadPath: string;
	readonly options: ConnectionOptions;
};

export class ConnectionConfig extends Context.Tag("s3qlite/ConnectionConfig")<
	ConnectionConfig,
	ConnectionConfigValue
>() {}

export class ConnectionState extends Context.Tag("s3qlite/ConnectionState")<
	ConnectionState,
	{
		readonly getConnection: Effect.Effect<Database>;
		readonly setConnection: (connection: Database) => Effect.Effect<void>;
	}
>() {}