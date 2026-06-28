import { copyFile, mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import packageJson from "../package.json";

const distPath = new URL("../dist/", import.meta.url);

const rewriteSpecifier = (specifier: string): string => {
	if (!specifier.startsWith("./") && !specifier.startsWith("../")) {
		return specifier;
	}

	if (specifier.endsWith(".json") || specifier.endsWith(".node")) {
		return specifier;
	}

	if (specifier.endsWith(".d.ts")) {
		return `${specifier.slice(0, -5)}.js`;
	}

	if (specifier.endsWith(".ts")) {
		return `${specifier.slice(0, -3)}.js`;
	}

	if (path.extname(specifier).length > 0) {
		return specifier;
	}

	return `${specifier}.js`;
};

const rewriteModuleSpecifiers = (source: string): string =>
	source
		.replace(
			/((?:import|export)\s[^\n;]*?from\s*["'])(\.?\.?\/[^"']+)(["'])/g,
			(_match, prefix: string, specifier: string, suffix: string) =>
				`${prefix}${rewriteSpecifier(specifier)}${suffix}`,
		)
		.replace(
			/(import\(\s*["'])(\.?\.?\/[^"']+)(["']\s*\))/g,
			(_match, prefix: string, specifier: string, suffix: string) =>
				`${prefix}${rewriteSpecifier(specifier)}${suffix}`,
		);

const rewriteFile = async (filePath: string): Promise<void> => {
	const source = await readFile(filePath, "utf8");
	const rewritten = rewriteModuleSpecifiers(source);

	if (rewritten !== source) {
		await writeFile(filePath, rewritten);
	}
};

const walk = async (directory: string): Promise<void> => {
	const entries = await readdir(directory, { withFileTypes: true });

	for (const entry of entries) {
		const fullPath = path.join(directory, entry.name);

		if (entry.isDirectory()) {
			await walk(fullPath);
			continue;
		}

		if (entry.name.endsWith(".js") || entry.name.endsWith(".d.ts")) {
			await rewriteFile(fullPath);
		}
	}
};

const distPackageJson = {
	name: packageJson.name,
	version: packageJson.version,
	type: "module",
	sideEffects: false,
	dependencies: packageJson.dependencies,
	peerDependencies: packageJson.peerDependencies,
	main: "./cjs/index.js",
	module: "./esm/index.js",
	types: "./dts/index.d.ts",
	exports: {
		"./package.json": "./package.json",
		".": {
			types: "./dts/index.d.ts",
			import: "./esm/index.js",
			default: "./cjs/index.js",
		},
		"./effect": {
			types: "./dts/effect.d.ts",
			import: "./esm/effect.js",
			default: "./cjs/effect.js",
		},
	},
};

const distPackageJsonText = JSON.stringify(distPackageJson, (_key, value: unknown) => value, "\t");

for (const folder of ["esm", "dts"]) {
	await walk(path.join(fileURLToPath(distPath), folder));
}

await mkdir(new URL("../dist/cjs/", import.meta.url), { recursive: true });
await writeFile(
	new URL("../dist/cjs/package.json", import.meta.url),
	'{\n\t"type": "commonjs"\n}\n',
);
await copyFile(
	new URL("../../../README.md", import.meta.url),
	new URL("../dist/README.md", import.meta.url),
);
await writeFile(new URL("../dist/package.json", import.meta.url), `${distPackageJsonText}\n`);