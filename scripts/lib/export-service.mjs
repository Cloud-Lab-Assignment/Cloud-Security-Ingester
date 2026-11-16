#!/usr/bin/env node
// Writes one self-contained service directory for the group repo.
// Used by scripts/export-group-repo.sh; reads the repo, never writes to it.
//
//   node scripts/lib/export-service.mjs export <repoRoot> <service> <outDir>
//   node scripts/lib/export-service.mjs check  <outDir>
//
// export: copies the service (minus node_modules, Dockerfile, package.json, .env),
//   vendors the @core/* workspace packages it uses into lib/<name>/, rewrites
//   imports and relative paths that point outside the service (proto/, packages/),
//   and writes package.json, Dockerfile, .dockerignore and README.md.
// check: every relative import / new URL(...) in the exported tree resolves to a
//   file inside it, every bare import is a builtin or a declared dependency, and
//   no "@core/" text is left.
import fs from 'node:fs';
import path from 'node:path';
import { builtinModules } from 'node:module';

const SCOPE = '@core/';
const SKIP_TOP = new Set(['node_modules', 'Dockerfile', 'package.json', 'package-lock.json', 'README.md', '.dockerignore']);

const DESCRIPTIONS = {
  'event-store':
    'Event Store: consumes raw and normalized security events from Kafka, stores them in DynamoDB ' +
    '(raw payloads archived to S3), serves them over REST, and is a 3PC participant for incident commits.',
  'incident-management':
    'Incident Management: owns incidents in Postgres, exposes a gRPC API (incident.v1) for the ' +
    'correlation engine and a REST API, publishes incident updates to Kafka, and coordinates the 3PC ' +
    'commit with the Event Store.',
  'correlation-engine':
    'Correlation Engine: consumes security alerts from Kafka, groups them per entity in a time window, ' +
    'enriches them from the Event Store and upserts incidents in Incident Management over gRPC.',
};

function die(msg) {
  console.error(`export-service: ${msg}`);
  process.exit(1);
}

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}

function walk(dir, out = []) {
  for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
    if (ent.name === 'node_modules') continue;
    const p = path.join(dir, ent.name);
    if (ent.isDirectory()) walk(p, out);
    else if (ent.isFile()) out.push(p);
  }
  return out;
}

function isInside(child, parent) {
  const rel = path.relative(parent, child);
  return rel === '' || (!rel.startsWith('..') && !path.isAbsolute(rel));
}

function relSpec(fromFile, toPath, keepTrailingSlash) {
  let rel = path.relative(path.dirname(fromFile), toPath).split(path.sep).join('/');
  if (!rel.startsWith('.')) rel = `./${rel}`;
  if (keepTrailingSlash && !rel.endsWith('/')) rel += '/';
  return rel;
}

// @core/<x> name -> { name, dir, srcDir, entry (relative to srcDir), deps }
function workspacePackages(repoRoot) {
  const map = new Map();
  const pkgsDir = path.join(repoRoot, 'packages');
  for (const ent of fs.readdirSync(pkgsDir, { withFileTypes: true })) {
    if (!ent.isDirectory()) continue;
    const dir = path.join(pkgsDir, ent.name);
    const pjFile = path.join(dir, 'package.json');
    if (!fs.existsSync(pjFile)) continue;
    const pj = readJson(pjFile);
    if (!pj.name?.startsWith(SCOPE)) continue;
    const exp = typeof pj.exports === 'string' ? pj.exports : pj.exports?.['.'];
    const entry = path.resolve(dir, exp || pj.main || 'index.js');
    const srcDir = path.join(dir, 'src');
    if (!isInside(entry, srcDir)) die(`${pj.name}: entry ${entry} is not under src/`);
    map.set(pj.name, {
      name: pj.name,
      libName: pj.name.slice(SCOPE.length),
      dir,
      srcDir,
      entry: path.relative(srcDir, entry),
      deps: pj.dependencies || {},
    });
  }
  return map;
}

const JS_EXT = /\.(m?js|cjs)$/;
const CORE_LITERAL = /(['"])(@core\/[A-Za-z0-9._-]+)(\/[^'"]*)?\1/g;
const REL_LITERAL = /(['"])(\.\.\/[^'"\n]*)\1/g;

function coreUses(text) {
  const used = new Set();
  for (const m of text.matchAll(CORE_LITERAL)) used.add(m[2]);
  return used;
}

function cmdExport(repoRoot, service, outDir) {
  repoRoot = path.resolve(repoRoot);
  outDir = path.resolve(outDir);
  const svcDir = path.join(repoRoot, 'services', service);
  const protoDir = path.join(repoRoot, 'proto');
  if (!fs.existsSync(path.join(svcDir, 'package.json'))) die(`no service at ${svcDir}`);
  const svcPj = readJson(path.join(svcDir, 'package.json'));
  const pkgs = workspacePackages(repoRoot);

  // 1. Plan the copies: [sourceFile, destFile, sourceRoot (what may be referenced freely)].
  const copies = [];
  for (const ent of fs.readdirSync(svcDir, { withFileTypes: true })) {
    if (SKIP_TOP.has(ent.name)) continue;
    if (ent.name.startsWith('.env') && ent.name !== '.env.example') continue;
    const p = path.join(svcDir, ent.name);
    const files = ent.isDirectory() ? walk(p) : ent.isFile() ? [p] : [];
    for (const f of files) copies.push({ src: f, dest: path.join(outDir, path.relative(svcDir, f)), root: svcDir });
  }

  // 2. Which @core packages are needed (package.json deps + actual imports, transitively).
  const needed = new Set(Object.keys(svcPj.dependencies || {}).filter((d) => d.startsWith(SCOPE)));
  for (const c of copies) if (JS_EXT.test(c.src)) for (const u of coreUses(fs.readFileSync(c.src, 'utf8'))) needed.add(u);
  const queue = [...needed];
  const libs = new Map();
  while (queue.length) {
    const name = queue.shift();
    if (libs.has(name)) continue;
    const pkg = pkgs.get(name);
    if (!pkg) die(`${service} uses ${name}, but no packages/* declares it`);
    libs.set(name, pkg);
    for (const f of walk(pkg.srcDir)) {
      copies.push({ src: f, dest: path.join(outDir, 'lib', pkg.libName, path.relative(pkg.srcDir, f)), root: pkg.srcDir });
      if (JS_EXT.test(f)) for (const u of coreUses(fs.readFileSync(f, 'utf8'))) if (!libs.has(u)) queue.push(u);
    }
  }

  // 3. Copy with rewrites.
  let usesProto = false;
  const notes = [];
  for (const c of copies) {
    fs.mkdirSync(path.dirname(c.dest), { recursive: true });
    if (!JS_EXT.test(c.src)) {
      let text = fs.readFileSync(c.src);
      if (path.basename(c.src) === '.env.example') {
        text = Buffer.from(text.toString('utf8').replaceAll('<repo>/proto/', '<service dir>/proto/'));
      }
      fs.writeFileSync(c.dest, text);
      continue;
    }
    let text = fs.readFileSync(c.src, 'utf8');
    let protoRewritten = false;

    // '@core/x' (and '@core/x/sub') -> relative path into lib/x.
    text = text.replace(CORE_LITERAL, (whole, q, name, sub) => {
      const pkg = libs.get(name);
      if (!pkg) die(`${c.src}: ${name} not vendored`);
      const target = sub
        ? path.join(outDir, 'lib', pkg.libName, sub.slice(1))
        : path.join(outDir, 'lib', pkg.libName, pkg.entry);
      return q + relSpec(c.dest, target, false) + q;
    });

    // Relative literals that leave the source root: proto/ or packages/<x>/src.
    text = text.replace(REL_LITERAL, (whole, q, spec) => {
      const resolved = path.resolve(path.dirname(c.src), spec);
      if (isInside(resolved, c.root)) return whole;
      const slash = spec.endsWith('/');
      if (isInside(resolved, protoDir)) {
        usesProto = true;
        protoRewritten = true;
        return q + relSpec(c.dest, path.join(outDir, 'proto', path.relative(protoDir, resolved)), slash) + q;
      }
      for (const pkg of libs.values()) {
        if (isInside(resolved, pkg.srcDir)) {
          return q + relSpec(c.dest, path.join(outDir, 'lib', pkg.libName, path.relative(pkg.srcDir, resolved)), slash) + q;
        }
      }
      die(`${path.relative(repoRoot, c.src)}: '${spec}' points outside the service and cannot be exported`);
    });

    // Comments that described the old layout of the proto path.
    if (protoRewritten) {
      const lines = text.split('\n');
      const out = [];
      let replaced = false;
      for (const line of lines) {
        if (/^\s*\/\/.*(levels? (above|up)|\/app\/services|\/app\/proto|Repo layout|image layout)/i.test(line)) {
          if (!replaced) {
            const indent = line.match(/^\s*/)[0];
            out.push(`${indent}// Exported layout: proto/ sits next to src/ in the service directory (/app/proto in the image).`);
            replaced = true;
          }
          continue;
        }
        out.push(line);
      }
      text = out.join('\n');
      if (replaced) notes.push(`rewrote proto layout comment in ${path.relative(outDir, c.dest)}`);
    }

    // Remaining mentions (comments): "@core/x" -> "lib/x".
    text = text.replace(/@core\/([A-Za-z0-9._-]+)/g, 'lib/$1');
    fs.writeFileSync(c.dest, text);
  }

  if (usesProto) {
    for (const f of walk(protoDir)) {
      const dest = path.join(outDir, 'proto', path.relative(protoDir, f));
      fs.mkdirSync(path.dirname(dest), { recursive: true });
      fs.copyFileSync(f, dest);
    }
  }

  // 4. package.json: own deps minus @core/*, plus the vendored packages' deps.
  const deps = {};
  for (const [k, v] of Object.entries(svcPj.dependencies || {})) if (!k.startsWith(SCOPE)) deps[k] = v;
  for (const pkg of libs.values()) {
    for (const [k, v] of Object.entries(pkg.deps)) {
      if (k.startsWith(SCOPE)) continue;
      if (deps[k] && deps[k] !== v) notes.push(`${k}: keeping ${service}'s range ${deps[k]} over ${pkg.name}'s ${v}`);
      else deps[k] = v;
    }
  }
  const sortedDeps = Object.fromEntries(Object.entries(deps).sort(([a], [b]) => a.localeCompare(b)));
  const pj = {
    name: service,
    version: svcPj.version || '1.0.0',
    private: true,
    type: 'module',
    main: 'src/server.js',
    engines: { node: '>=22' },
    scripts: { test: 'node --test "test/**/*.test.js"' },
    dependencies: sortedDeps,
  };
  if (svcPj.devDependencies) {
    const dev = Object.fromEntries(Object.entries(svcPj.devDependencies).filter(([k]) => !k.startsWith(SCOPE)));
    if (Object.keys(dev).length) pj.devDependencies = dev;
  }
  fs.writeFileSync(path.join(outDir, 'package.json'), `${JSON.stringify(pj, null, 2)}\n`);

  // 5. Dockerfile (build context = this directory).
  const srcDocker = fs.readFileSync(path.join(svcDir, 'Dockerfile'), 'utf8').split('\n');
  const rdsLines = srcDocker.filter((l) => /rds/i.test(l));
  const exposeIdx = srcDocker.findIndex((l) => /^EXPOSE\s/.test(l));
  if (exposeIdx < 0) die(`${service}/Dockerfile has no EXPOSE`);
  const exposeComment = exposeIdx > 0 && /^#/.test(srcDocker[exposeIdx - 1]) ? srcDocker[exposeIdx - 1] : null;
  const exposeLine = srcDocker[exposeIdx];
  const ports = exposeLine.replace(/^EXPOSE\s+/, '').trim().split(/\s+/);

  const runtimeDirs = fs
    .readdirSync(outDir, { withFileTypes: true })
    .filter((e) => e.isDirectory() && e.name !== 'test' && e.name !== 'node_modules')
    .map((e) => e.name)
    .sort((a, b) => (a === 'src' ? -1 : b === 'src' ? 1 : a.localeCompare(b)));

  const df = [
    `# Build context is this directory. From the group repo root:`,
    `#   docker build -t ${service} services/${service}`,
    `FROM node:22-alpine`,
    ``,
    `WORKDIR /app`,
    `ENV NODE_ENV=production`,
    ``,
  ];
  if (rdsLines.length) df.push(...rdsLines, '');
  df.push(
    `# --chmod: files must be readable by USER node whatever the host umask was.`,
    `COPY --chmod=a+rX package.json package-lock.json ./`,
    `RUN npm ci --omit=dev && npm cache clean --force`,
    ``,
    ...runtimeDirs.map((d) => `COPY --chmod=a+rX ${d}/ ${d}/`),
    ``,
    `USER node`,
  );
  if (exposeComment) df.push(exposeComment);
  df.push(exposeLine, `CMD ["node", "src/server.js"]`, ``);
  fs.writeFileSync(path.join(outDir, 'Dockerfile'), df.join('\n'));

  fs.writeFileSync(path.join(outDir, '.dockerignore'), ['node_modules', 'test', '.env*', '!.env.example', ''].join('\n'));

  // 6. README.md
  const libList = [...libs.values()].map((p) => `\`lib/${p.libName}/\`: vendored from \`packages/${path.basename(p.dir)}/src\` of the workspace repo`);
  const readme = [
    `# ${service}`,
    ``,
    DESCRIPTIONS[service] || `${service} core service.`,
    ``,
    `Exported from the core-services workspace repo (https://github.com/rish-kun/cloud-computing-assignment)`,
    `by \`scripts/export-group-repo.sh\`. Make code changes there and re-export; this directory is generated.`,
    ``,
    `## Build`,
    ``,
    `The build context is this directory. From the group repo root:`,
    ``,
    '```sh',
    `docker build -t ${service} services/${service}`,
    '```',
    ``,
    `## Ports`,
    ``,
    ...ports.map((p) => `- \`${p}\``),
    ...(exposeComment ? [``, `(${exposeComment.replace(/^#\s*/, '')})`] : []),
    ``,
    `HTTP endpoints \`GET /health\`, \`GET /ready\` and \`GET /metrics\` are served on the HTTP port.`,
    ``,
    `## Layout`,
    ``,
    `- \`src/\`: service code (\`src/server.js\` is the entry point)`,
    `- \`test/\`: unit tests (\`npm ci && npm test\`; integration tests run only with \`INTEGRATION=1\`)`,
    ...libList.map((l) => `- ${l}`),
    ...(usesProto ? [`- \`proto/\`: gRPC contract (\`incident.v1\`); override with \`PROTO_PATH\``] : []),
    ...(fs.existsSync(path.join(outDir, 'migrations')) ? [`- \`migrations/\`: Postgres migrations, applied at startup`] : []),
    `- \`.env.example\`: configuration (dummy values)`,
    ``,
    `## More`,
    ``,
    `Environment variables, Kafka topics, AWS resources, K8s notes and the 3PC demo are in`,
    `\`docs/handoff.md\` of the core-services repo (section "${service}").`,
    ``,
  ];
  fs.writeFileSync(path.join(outDir, 'README.md'), readme.join('\n'));

  for (const n of notes) console.error(`export-service: ${service}: ${n}`);
  console.log(
    `${service}: ${copies.length} files, libs=[${[...libs.values()].map((p) => p.libName).join(',')}], proto=${usesProto}, ports=${ports.join(',')}`,
  );
}

const STATIC_IMPORT = /(?:^|[^.\w$])(?:import|export)\s[^'"]*?from\s*(['"])([^'"]+)\1|(?:^|[^.\w$])import\s*(['"])([^'"]+)\3/g;
const DYNAMIC_IMPORT = /import\(\s*(['"])([^'"]+)\1\s*\)/g;
const URL_LITERAL = /new URL\(\s*(['"])([^'"]+)\1\s*,\s*import\.meta\.url/g;

function cmdCheck(outDir) {
  outDir = path.resolve(outDir);
  const pj = readJson(path.join(outDir, 'package.json'));
  const declared = new Set([...Object.keys(pj.dependencies || {}), ...Object.keys(pj.devDependencies || {})]);
  const builtins = new Set(builtinModules);
  const problems = [];
  for (const f of walk(outDir)) {
    const text = fs.readFileSync(f, 'utf8');
    if (text.includes(SCOPE)) problems.push(`${path.relative(outDir, f)}: still mentions ${SCOPE}`);
    if (!JS_EXT.test(f)) continue;
    const specs = [];
    for (const m of text.matchAll(STATIC_IMPORT)) specs.push(m[2] ?? m[4]);
    for (const m of text.matchAll(DYNAMIC_IMPORT)) specs.push(m[2]);
    for (const spec of specs) {
      if (spec.startsWith('.')) {
        const r = path.resolve(path.dirname(f), spec);
        if (!isInside(r, outDir)) problems.push(`${path.relative(outDir, f)}: import '${spec}' leaves the service`);
        else if (!fs.existsSync(r)) problems.push(`${path.relative(outDir, f)}: import '${spec}' does not exist`);
      } else if (spec.startsWith('node:') || builtins.has(spec)) {
        // builtin
      } else {
        const name = spec.startsWith('@') ? spec.split('/').slice(0, 2).join('/') : spec.split('/')[0];
        if (!declared.has(name)) problems.push(`${path.relative(outDir, f)}: '${spec}' is not a declared dependency`);
      }
    }
    for (const m of text.matchAll(URL_LITERAL)) {
      const spec = m[2];
      if (!spec.startsWith('.')) continue;
      const r = path.resolve(path.dirname(f), spec);
      if (!isInside(r, outDir)) problems.push(`${path.relative(outDir, f)}: new URL('${spec}') leaves the service`);
      else if (!fs.existsSync(r)) problems.push(`${path.relative(outDir, f)}: new URL('${spec}') does not exist`);
    }
  }
  if (problems.length) {
    for (const p of problems) console.error(`export-service: check: ${p}`);
    process.exit(1);
  }
}

const [cmd, ...args] = process.argv.slice(2);
if (cmd === 'export' && args.length === 3) cmdExport(...args);
else if (cmd === 'check' && args.length === 1) cmdCheck(...args);
else die('usage: export-service.mjs export <repoRoot> <service> <outDir> | check <outDir>');
