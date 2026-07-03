const WRAPPER_PROPERTIES = new Set(["fn", "fnUntraced"]);

const isEffectFnMember = (node) =>
  node.type === "MemberExpression" &&
  !node.computed &&
  node.object.type === "Identifier" &&
  node.object.name === "Effect" &&
  node.property.type === "Identifier" &&
  WRAPPER_PROPERTIES.has(node.property.name);

const isSignatureTransparentCallee = (callee) =>
  isEffectFnMember(callee) || (callee.type === "CallExpression" && isEffectFnMember(callee.callee));

const positionalParams = (params) => params.filter((param) => !(param.type === "Identifier" && param.name === "this"));

const shouldReport = (node) => {
  let child = node;
  let parent = node.parent;
  while (
    parent &&
    (parent.type === "Property" ||
      parent.type === "ObjectExpression" ||
      parent.type === "ArrayExpression" ||
      parent.type === "SpreadElement")
  ) {
    child = parent;
    parent = parent.parent;
  }
  if (
    parent &&
    (parent.type === "CallExpression" || parent.type === "NewExpression") &&
    parent.arguments.includes(child)
  ) {
    return isSignatureTransparentCallee(parent.callee);
  }
  return true;
};

const moduleSubjectName = (filename) => {
  const base = filename.split(/[/\\]/).at(-1) ?? "";
  return base.replace(/\.[^.]+$/, "");
};

const isDataFirstOperator = (params, subject) => {
  if (params.length !== 2) {
    return false;
  }
  const annotation = params[0].typeAnnotation?.typeAnnotation;
  return (
    annotation?.type === "TSTypeReference" &&
    annotation.typeName.type === "Identifier" &&
    annotation.typeName.name === subject
  );
};

const maxPositionalParams = {
  create(context) {
    const subject = moduleSubjectName(context.filename ?? context.getFilename());
    const check = (node) => {
      const params = positionalParams(node.params);
      if (params.length <= 1) {
        return;
      }
      if (isDataFirstOperator(params, subject)) {
        return;
      }
      if (!shouldReport(node)) {
        return;
      }
      context.report({
        message: `Function takes ${params.length} positional parameters — accept a single options object instead.`,
        node: params[1],
      });
    };
    return {
      FunctionDeclaration: check,
      FunctionExpression: check,
      ArrowFunctionExpression: check,
    };
  },
};

const plugin = {
  meta: {
    name: "resonate",
  },
  rules: {
    "max-positional-params": maxPositionalParams,
  },
};

export default plugin;
