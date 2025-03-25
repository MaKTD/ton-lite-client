module.exports = {
  testTimeout: 20000,
  maxConcurrency: 1,
  preset: 'ts-jest/presets/default-esm',
  globals: {
    'ts-jest': {
      isolatedModules: false,
      useESM: true,
      tsconfig: './tsconfig.jest.json'
    }
  },
  extensionsToTreatAsEsm: ['.ts'],
  moduleNameMapper: {
    '^(\\.{1,2}/.*)\\.js$': '$1'
  },
  clearMocks: true,
  forceExit: false,
  moduleFileExtensions: [
    'js',
    'json',
    'ts',
    'mjs',
    'cjs',
  ],
  rootDir: './',
  testRegex: '.*\\.spec\\.ts$',
  modulePathIgnorePatterns: [
    '<rootDir>/temp/',
    '<rootDir>/dist/',
  ],
  transformIgnorePatterns: [],
  collectCoverageFrom: [
    '**/*.(t|j)s',
  ],
  coverageDirectory: './coverage',
  testEnvironment: 'jest-environment-node',
  testPathIgnorePatterns: [
    '/node_modules/',
  ],
};
