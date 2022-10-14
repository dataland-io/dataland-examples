module.exports = {
  ignorePatterns: [
    ".eslintrc.js",
    "jest.config.js",
    "dist/**",
    "node_modules/**",
  ],
  parser: "@typescript-eslint/parser",
  parserOptions: {
    project: "./tsconfig.json",
    tsconfigRootDir: __dirname,
  },
  plugins: ["@typescript-eslint", "import"],
  extends: [
    "eslint:recommended",
    "plugin:import/recommended",
    "plugin:import/typescript",
    "plugin:@typescript-eslint/recommended",
    "prettier",
  ],
  rules: {
    eqeqeq: ["warn", "always", { null: "ignore" }],
    "@typescript-eslint/no-explicit-any": "off",
    "@typescript-eslint/no-non-null-assertion": "off",
    "@typescript-eslint/no-empty-interface": "off",
    "@typescript-eslint/no-unused-vars": [
      "warn",
      {
        vars: "all",
        args: "all",
        argsIgnorePattern: "^_",
      },
    ],
    "@typescript-eslint/strict-boolean-expressions": [
      "warn",
      {
        allowString: false,
        allowNumber: false,
        allowNullableObject: false,
      },
    ],
    "import/order": [
      "warn",
      {
        "newlines-between": "never",
        alphabetize: { order: "asc" },
      },
    ],
    // prevent exports like `export default someValue` to ensure the consumer knows what it's importing
    // especially important when refactoring and the meaning of the exported value changes
    "import/no-default-export": "warn",
    "import/no-extraneous-dependencies": [
      "warn",
      {
        devDependencies: false,
        optionalDependencies: false,
        peerDependencies: false,
      },
    ],
    // per https://github.com/benmosher/eslint-plugin-import/issues/2085
    "sort-imports": ["warn", { ignoreDeclarationSort: true }],
  },
};
