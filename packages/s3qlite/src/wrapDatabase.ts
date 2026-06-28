const asyncDatabaseMethods = new Set<PropertyKey>([
	"all",
	"close",
	"exec",
	"get",
	"iterate",
	"pragma",
	"run",
]);

const asyncStatementMethods = new Set<PropertyKey>(["all", "close", "get", "run"]);

const syncStatementMethods = new Set<PropertyKey>(["bind", "pluck", "raw", "safeIntegers"]);

const wrapTransaction = <T extends (...args: readonly unknown[]) => unknown>(
	transaction: T,
	wrap: <R>(call: () => R, method: PropertyKey, args: readonly unknown[]) => R,
): T =>
	new Proxy(transaction, {
		apply: (target, thisArg, args) =>
			wrap(() => Reflect.apply(target, thisArg, args), "transaction", args),
		get: (target, property, receiver) => {
			const value = Reflect.get(target, property, receiver);

			if (typeof value !== "function") {
				return value;
			}

			return wrapTransaction(value as (...args: readonly unknown[]) => unknown, wrap);
		},
	}) as T;

const wrapStatement = <T extends object>(
	statement: T,
	wrap: <R>(call: () => R, method: PropertyKey, args: readonly unknown[]) => R,
): T => {
	let proxy: T;

	proxy = new Proxy(statement, {
		get: (target, property, receiver) => {
			const value = Reflect.get(target, property, receiver);

			if (typeof value !== "function") {
				return value;
			}

			if (syncStatementMethods.has(property)) {
				return (...args: unknown[]) => {
					Reflect.apply(value, target, args);
					return proxy;
				};
			}

			if (property === "iterate") {
				return (...args: unknown[]) =>
					(async function* () {
						const iterable = wrap(
							() => Reflect.apply(value, target, args),
							property,
							args,
						) as AsyncIterable<unknown>;

						for await (const item of iterable) {
							yield item;
						}
					})();
			}

			if (asyncStatementMethods.has(property)) {
				return (...args: unknown[]) =>
					wrap(() => Reflect.apply(value, target, args), property, args);
			}

			return (...args: unknown[]) => Reflect.apply(value, target, args);
		},
	});

	return proxy;
};

export const wrapDatabase = <T extends object>(
	getTarget: () => T,
	wrap: <R>(call: () => R, method: PropertyKey, args: readonly unknown[]) => R,
): T => {
	return new Proxy({} as T, {
		get: (wrapper, property, receiver) => {
			if (Reflect.has(wrapper, property)) {
				return Reflect.get(wrapper, property, receiver);
			}

			const original = getTarget();
			const value = Reflect.get(original, property, original);

			if (typeof value !== "function") {
				return value;
			}

			if (property === "prepare") {
				return (...args: unknown[]) =>
					wrapStatement(Reflect.apply(value, getTarget(), args) as object, wrap);
			}

			if (property === "transaction") {
				return (...args: unknown[]) =>
					wrapTransaction(
						Reflect.apply(value, getTarget(), args) as (
							...args: readonly unknown[]
						) => unknown,
						wrap,
					);
			}

			if (asyncDatabaseMethods.has(property)) {
				return (...args: unknown[]) =>
					wrap(
						() => {
							const target = getTarget();
							const current = Reflect.get(target, property, target) as (
								...args: readonly unknown[]
							) => unknown;
							return Reflect.apply(current, target, args);
						},
						property,
						args,
					);
			}

			return (...args: unknown[]) => {
				const target = getTarget();
				const current = Reflect.get(target, property, target) as (
					...args: readonly unknown[]
				) => unknown;
				return Reflect.apply(current, target, args);
			};
		},
	});
};