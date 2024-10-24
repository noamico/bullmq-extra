module.exports = {
  moduleFileExtensions: ['js', 'json', 'ts'],
  rootDir: '.',
  testEnvironment: 'node',
  testRegex: '.*test\\.ts$',
  transform: {
    '^.+\\.(t|j)s$': 'ts-jest',
  },
  transformIgnorePatterns: ['/node_modules/'],
  coverageDirectory: './coverage',
  coverageReporters: [
    'json-summary',
    'text-summary',
    'cobertura',
    'lcov',
    'text',
  ],
  collectCoverage: true,
  collectCoverageFrom: ['src/**/*.ts', '!src/**/*test.ts', '!src/index.ts'],
  forceExit: true,
};
