module.exports = {
    parserOptions: {
        project: './tsconfig.json'
    },
    root: true,
    parser: '@typescript-eslint/parser',
    env: {
        node: true,
        jest: true,
        commonjs: true,
        es2021: true,
    },
    ignorePatterns: ['.eslintrc.cjs', './node_modules', '**/*.md', 'dist/**', 'dev/**'],
    overrides: [
        {
            files: ['**/*.ts', '**/*.js', '**/*.cjs', '**/*.mjs'],
            parser: '@typescript-eslint/parser',
            plugins: [
                '@typescript-eslint',
                'import',
                'import-newlines',
                'angular-file-naming',
                'switch-case',
                'no-null',
                'sequel',
            ],
            extends: [
                'plugin:node/recommended',
                'airbnb-typescript/base',
                'plugin:import/recommended',
                'plugin:import/typescript',
            ],
            rules: {
                'sequel/function-case': 'warn',
                'sequel/indent': ['warn', 2],
                'sequel/max-placeholders': ['warn', { 'max': 8 }],
                'sequel/no-eol-command': ['warn', { 'allowOnOwnLine': true }],
                'sequel/no-shorthand-all': [
                    'error',
                    { 'allowQualified': true, 'allowCountAll': true },
                ],
                'sequel/no-unsafe-query': 'error',
                'sequel/spacing': 'warn',
                'no-null/no-null': 'off',
                'node/no-extraneous-import': "off",
                'import/no-extraneous-dependencies': "off",
                'switch-case/newline-between-switch-case': ['error', 'always', { 'fallthrough': 'never' }],
                '@typescript-eslint/interface-name-prefix': 'off',
                '@typescript-eslint/explicit-function-return-type': 'off',
                '@typescript-eslint/explicit-module-boundary-types': 'off',
                '@typescript-eslint/no-explicit-any': 'off',
                'eqeqeq': ['error', 'always', { 'null': 'ignore' }],
                'import/prefer-default-export': 'off',
                'import/first': 'warn',
                'import/no-mutable-exports': 'warn',
                'import/no-unresolved': 'error',
                'import/no-cycle': 'error',
                'import/no-duplicates': 'error',
                'import/newline-after-import': ['error', { count: 1 }],
                'sort-imports': ['error', {
                    'ignoreCase': true,
                    'ignoreDeclarationSort': true,
                }],
                'import/order': ['error', {
                    'groups': ['builtin', 'external', 'internal', 'sibling', 'parent', 'index'],
                    'alphabetize': { 'order': 'asc' },
                }],
                'import-newlines/enforce': [
                    'error',
                    {
                        'items': 2,
                    },
                ],
                'object-curly-newline': ['error', {
                    'ImportDeclaration': { 'multiline': true, 'minProperties': 3 },
                    'ExportDeclaration': { 'multiline': true, 'minProperties': 3 },
                }],
                'no-unused-expressions': 'error',
                'no-console': 'error',
                'no-underscore-dangle': 'off',
                'no-return-assign': 'error',
                'newline-before-return': 'error',
                'strict': 'off',
                'node/no-unsupported-features/es-syntax': ['error', {
                    'ignores': ['modules'],
                }],
                'node/no-missing-import': ['warn', {
                    'tryExtensions': ['.js', '.json', '.node', '.ts'],
                }],
                'max-len': [
                    'error',
                    { 'code': 180, 'comments': 180 },
                ],
                'no-multiple-empty-lines': ['error', { max: 1 }],
                'no-restricted-syntax': [
                    'error',
                    'ForInStatement',
                    'LabeledStatement',
                    'WithStatement',
                ],
                '@typescript-eslint/lines-between-class-members': ['error', 'always', { 'exceptAfterSingleLine': true }],
                'max-classes-per-file': 'off',
                '@typescript-eslint/no-throw-literal': 'off',
                'node/no-unpublished-import': 'off',
                'no-return-await': 'warn',
                'no-trailing-spaces': ['warn', { 'ignoreComments': true }],
                '@typescript-eslint/indent': [
                    'error',
                    2,
                    {
                        'SwitchCase': 1,
                        'ignoredNodes': [
                            'FunctionExpression > .params[decorators.length > 0]',
                            'FunctionExpression > .params > :matches(Decorator, :not(:first-child))',
                            'ClassBody.body > PropertyDefinition[decorators.length > 0] > .key',
                        ],
                    },
                ],
                'angular-file-naming/service-filename-suffix': [
                    'warn',
                    {
                        'suffixes': [
                            'service',
                            'repo',
                            'interceptor',
                            'decorator',
                            'strategy',
                            'gateway',
                            'api',
                            'interface',
                            'model',
                            'guard',
                        ],
                    },
                ],
                '@typescript-eslint/no-namespace': 'error',
                '@typescript-eslint/no-floating-promises': 'error',
                '@typescript-eslint/no-misused-promises': 'error',
                '@typescript-eslint/promise-function-async': 'warn',
            },
            overrides: [
                {
                    'files': ['**/*.dto.ts'],
                    'rules': {
                        '--no-useless-constructor': 'off',
                        '@typescript-eslint/no-useless-constructor': 'off',
                    },
                },
                {
                    'files': ['**/tests/**/*.spec.ts'],
                    'rules': {
                        'func-names': 'off',
                    },
                },
            ],
        },
    ],
};
