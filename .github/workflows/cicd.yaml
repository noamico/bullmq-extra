name: CI/CD Pipeline

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  build-and-publish:
    runs-on: ubuntu-latest
    permissions:
      contents: write
      packages: write

    steps:
      - name: Checkout Repository
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Semver Check
        uses: cocogitto/cocogitto-action@v3
        with:
          check-latest-tag-only: true

      - name: Set up Node.js
        uses: actions/setup-node@v4
        with:
          node-version: '20.18'

      - name: Configure NPM
        run: echo "NPM_TOKEN=${{ secrets.NPM_TOKEN }}" >> $GITHUB_ENV

      - name: Install Dependencies
        run: npm ci

      - name: Build
        run: |
          npm run build

      - name: Run Tests
        run: |
           npm test

      - name: Semver release
        if: github.ref == 'refs/heads/main'
        uses: cocogitto/cocogitto-action@v3
        id: release
        with:
          check-latest-tag-only: true
          release: true
          git-user: 'Cog Bot'
          git-user-email: 'cogbot@appstack-io'

      - name: Update package.json version
        if: github.ref == 'refs/heads/main' && steps.release.outputs.version
        run: node update-version.js

      - name: Commit and push updated package.json
        if: github.ref == 'refs/heads/main' && steps.release.outputs.version
        run: |
          git config --local user.email "action@appstack-io"
          git config --local user.name "GitHub Action"
          git commit -am "chore: bump version to $(cat package.json | jq -r .version)"
          git push

      - name: Publish
        if: github.ref == 'refs/heads/main' && steps.release.outputs.version
        run: |
          echo $APPSTACKIO_NPM_TOKEN
          cat .npmrc
          npm publish
