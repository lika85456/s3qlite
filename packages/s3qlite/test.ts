import { connect } from "@tursodatabase/database";
//
const c = await connect("./test.db");
// we need to snapshot/checkpoint before closing
// await c.run("PRAGMA wal_checkpoint(TRUNCATE);");
// create database users, with (id, name) and insert single user
await c.run("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)");
await c.run("INSERT INTO users (name) VALUES (?)", ["Alice"]);
await c.close();
console.log("Done");

// import { layer } from "@effect/platform-node/NodeFileSystem";
// import { FileSystem } from "@effect/platform/FileSystem";
// import { Effect } from "effect";
//
// // const c = await connect("./test.db");
// await Effect.runPromise(
// 	Effect.gen(function* () {
// 		const fs = yield* FileSystem;
// 		const contents = yield* fs.readFileString("./te");
// 		console.log(contents);
// 	}).pipe(Effect.provide(layer)),
// );