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

export class ConnectionConfig extends Context.Service<ConnectionConfig, ConnectionConfigValue>()(
	"s3qlite/ConnectionConfig",
) {}

export class ConnectionState extends Context.Service<
	ConnectionState,
	{
		readonly getConnection: Effect.Effect<Database>;
		/**
		 * Replaces the live connection. The caller is responsible for closing the previous one.
		 */
		readonly setConnection: (connection: Database) => Effect.Effect<void>;
	}
>()("s3qlite/ConnectionState") {}