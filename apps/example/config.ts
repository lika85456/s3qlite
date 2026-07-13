import { config as loadEnv } from "dotenv";
import { fileURLToPath } from "node:url";

loadEnv({ path: fileURLToPath(new URL("./.env", import.meta.url)) });

const getEnv = (name: string): string => {
	const value = process.env[name];
	if (!value) {
		throw new Error(`Missing ${name} in apps/example/.env`);
	}
	return value;
};

export const bucket = getEnv("S3_BUCKET");
export const endpoint = getEnv("S3_ENDPOINT");
export const region = getEnv("S3_REGION");
export const accessKeyId = getEnv("S3_ACCESS_KEY_ID");
export const secretAccessKey = getEnv("S3_SECRET_ACCESS_KEY");

export const s3 = {
	credentials: { accessKeyId, secretAccessKey },
	endpoint,
	forcePathStyle: true,
	region,
};

export const localDirectory = "/tmp/s3qlite/example";