const DEFAULT_MAX_LINES = 4;

const isFunctionExpression = (node) =>
	node && (node.type === "ArrowFunctionExpression" || node.type === "FunctionExpression");

const isExported = (node) => {
	const parent = node.parent;
	if (!parent) return false;
	if (parent.type === "ExportDefaultDeclaration" || parent.type === "ExportNamedDeclaration")
		return true;
	if (parent.type === "VariableDeclaration")
		return parent.parent?.type === "ExportNamedDeclaration";
	return false;
};

const getBodyLineCount = (fn) => {
	if (fn.body.type !== "BlockStatement") return fn.body.loc.end.line - fn.body.loc.start.line + 1;
	const [first] = fn.body.body;
	const last = fn.body.body.at(-1);
	if (!first || !last) return 0;
	return last.loc.end.line - first.loc.start.line + 1;
};

const getCandidate = (node) => {
	if (node.type === "FunctionDeclaration" && node.id && !isExported(node)) {
		return { id: node.id, fn: node, scopeNode: node.parent ?? node };
	}

	if (
		node.type !== "VariableDeclarator" ||
		node.id.type !== "Identifier" ||
		!isFunctionExpression(node.init)
	)
		return null;
	if (node.parent.kind !== "const" || isExported(node.parent)) return null;

	return { id: node.id, fn: node.init, scopeNode: node };
};

const getVariable = (sourceCode, candidate) => {
	let scope = sourceCode.getScope(candidate.scopeNode);
	while (scope) {
		const variable = scope.set.get(candidate.id.name);
		if (variable) return variable;
		scope = scope.upper;
	}
	return null;
};

const isSameNode = (left, right) =>
	left.range[0] === right.range[0] && left.range[1] === right.range[1];

const isWithin = (node, parent) =>
	node.range[0] >= parent.range[0] && node.range[1] <= parent.range[1];

const isDirectCallReference = (identifier) => {
	const parent = identifier.parent;
	return Boolean(
		parent &&
		(parent.type === "CallExpression" || parent.type === "NewExpression") &&
		parent.callee === identifier,
	);
};

const getMaxLines = (context) => context.options[0]?.maxLines ?? DEFAULT_MAX_LINES;

const isIdentifierNamed = (node, name) => node?.type === "Identifier" && node.name === name;

const unwrapExpression = (node) => {
	if (!node) return null;
	if (node.type !== "ArrowFunctionExpression") return node;
	if (node.body.type !== "BlockStatement") return node.body;
	if (node.body.body.length !== 1 || node.body.body[0].type !== "ReturnStatement") return null;
	return node.body.body[0].argument ?? null;
};

const isStringCallOn = (node, name) => {
	if (node?.type !== "CallExpression") return false;
	if (!isIdentifierNamed(node.callee, "String")) return false;
	return node.arguments.length === 1 && isIdentifierNamed(node.arguments[0], name);
};

const isPlainErrorWrap = (node, name) => {
	if (node?.type !== "NewExpression") return false;
	if (!isIdentifierNamed(node.callee, "Error")) return false;
	if (node.arguments.length !== 1) return false;
	return isStringCallOn(node.arguments[0], name);
};

const isInstanceofErrorCheck = (node, name) =>
	node?.type === "BinaryExpression" &&
	node.operator === "instanceof" &&
	isIdentifierNamed(node.left, name) &&
	isIdentifierNamed(node.right, "Error");

const isUselessEffectCatch = (fn) => {
	const errorName = fn.params[0]?.type === "Identifier" ? fn.params[0].name : null;
	if (!errorName) return false;

	const expression = unwrapExpression(fn);
	if (!expression) return false;
	if (isIdentifierNamed(expression, errorName)) return true;
	if (isPlainErrorWrap(expression, errorName)) return true;

	if (expression.type !== "ConditionalExpression") return false;
	if (!isInstanceofErrorCheck(expression.test, errorName)) return false;
	if (!isIdentifierNamed(expression.consequent, errorName)) return false;
	return isPlainErrorWrap(expression.alternate, errorName);
};

const getPropertyName = (property) => {
	if (property.key.type === "Identifier") return property.key.name;
	if (property.key.type === "Literal") return property.key.value;
	return null;
};

const isEffectTryCall = (node) => {
	if (node.type !== "CallExpression" || node.arguments.length === 0) return false;
	if (node.callee.type !== "MemberExpression" || node.callee.computed) return false;
	return (
		(isIdentifierNamed(node.callee.object, "Effect") &&
			isIdentifierNamed(node.callee.property, "try")) ||
		(isIdentifierNamed(node.callee.object, "Effect") &&
			isIdentifierNamed(node.callee.property, "tryPromise"))
	);
};

const getCatchProperty = (node) => {
	const config = node.arguments[0];
	if (config?.type !== "ObjectExpression") return null;
	return config.properties.find(
		(property) =>
			property.type === "Property" &&
			property.kind === "init" &&
			!property.computed &&
			getPropertyName(property) === "catch",
	);
};

const createRule = (context, shouldReport) => {
	const sourceCode = context.sourceCode;

	const check = (node) => {
		const candidate = getCandidate(node);
		if (!candidate) return;
		shouldReport(context, sourceCode, candidate);
	};

	return {
		FunctionDeclaration: check,
		VariableDeclarator: check,
	};
};

const isIgnoredUndefinedIdentifier = (node) => {
	const parent = node.parent;
	if (!parent) return false;

	if (parent.type === "MemberExpression" && !parent.computed && parent.property === node)
		return true;
	if (parent.type === "Property" && !parent.computed && parent.key === node) return true;
	if (parent.type === "MethodDefinition" && !parent.computed && parent.key === node) return true;
	if (parent.type === "PropertyDefinition" && !parent.computed && parent.key === node)
		return true;

	return false;
};

const functionMinimumLengthRule = {
	meta: {
		type: "suggestion",
		schema: [
			{
				type: "object",
				properties: {
					maxLines: { type: "number" },
				},
				additionalProperties: false,
			},
		],
		messages: {
			tooShort:
				"Function '{{name}}' has a body of {{lines}} lines. Inline it or make the abstraction earn its place.",
		},
	},
	create(context) {
		return createRule(context, (ruleContext, _sourceCode, candidate) => {
			const lines = getBodyLineCount(candidate.fn);
			if (lines > getMaxLines(ruleContext)) return;
			ruleContext.report({
				node: candidate.id,
				messageId: "tooShort",
				data: { name: candidate.id.name, lines: String(lines) },
			});
		});
	},
};

const noSingleUseFunctionRule = {
	meta: {
		type: "suggestion",
		schema: [
			{
				type: "object",
				properties: {
					maxLines: { type: "number" },
				},
				additionalProperties: false,
			},
		],
		messages: {
			singleUse:
				"Function '{{name}}' is only called once and its body is only {{lines}} lines. Inline it.",
		},
	},
	create(context) {
		return createRule(context, (ruleContext, sourceCode, candidate) => {
			const lines = getBodyLineCount(candidate.fn);
			if (lines > getMaxLines(ruleContext)) return;

			const variable = getVariable(sourceCode, candidate);
			if (!variable) return;

			const refs = variable.references.filter(
				(ref) => !isSameNode(ref.identifier, candidate.id),
			);
			if (refs.length !== 1) return;

			const [ref] = refs;
			if (isWithin(ref.identifier, candidate.fn.body)) return;
			if (!isDirectCallReference(ref.identifier)) return;

			ruleContext.report({
				node: candidate.id,
				messageId: "singleUse",
				data: { name: candidate.id.name, lines: String(lines) },
			});
		});
	},
};

const noUselessEffectCatchRule = {
	meta: {
		type: "suggestion",
		schema: [],
		messages: {
			uselessCatch:
				"This Effect catch does not add context. Remove it or map the error to something more descriptive.",
		},
	},
	create(context) {
		return {
			CallExpression(node) {
				if (!isEffectTryCall(node)) return;

				const catchProperty = getCatchProperty(node);
				if (!catchProperty) return;
				if (!isFunctionExpression(catchProperty.value)) return;
				if (!isUselessEffectCatch(catchProperty.value)) return;

				context.report({
					node: catchProperty,
					messageId: "uselessCatch",
				});
			},
		};
	},
};

const noNullUndefinedOptionRule = {
	meta: {
		type: "problem",
		schema: [],
		messages: {
			noNull: "Avoid 'null'. Model absence with Effect.Option instead.",
			noUndefined: "Avoid 'undefined'. Model absence with Effect.Option instead.",
		},
	},
	create(context) {
		return {
			Literal(node) {
				if (node.value !== null) return;
				context.report({
					node,
					messageId: "noNull",
				});
			},
			Identifier(node) {
				if (node.name !== "undefined" || isIgnoredUndefinedIdentifier(node)) return;
				context.report({
					node,
					messageId: "noUndefined",
				});
			},
			TSNullKeyword(node) {
				context.report({
					node,
					messageId: "noNull",
				});
			},
			TSUndefinedKeyword(node) {
				context.report({
					node,
					messageId: "noUndefined",
				});
			},
		};
	},
};

const noOxlintDisableCommentRule = {
	meta: {
		type: "suggestion",
		schema: [],
		messages: {
			noDisable:
				"Unless you are a real person you are not allowed to ignore linting rules - ask the user if its possible to ignore this rule instead.",
		},
	},
	create(context) {
		return {
			Program() {
				for (const comment of context.sourceCode.getAllComments()) {
					if (!/\boxlint-disable(?:-next-line|-line)?\b/.test(comment.value)) continue;
					context.report({
						loc: comment.loc,
						messageId: "noDisable",
					});
				}
			},
		};
	},
};

export default {
	meta: {
		name: "local-rules",
	},
	rules: {
		"function-minimum-length": functionMinimumLengthRule,
		"no-oxlint-disable-comment": noOxlintDisableCommentRule,
		"no-null-undefined-option": noNullUndefinedOptionRule,
		"no-useless-effect-catch": noUselessEffectCatchRule,
		"no-single-use-function": noSingleUseFunctionRule,
	},
};