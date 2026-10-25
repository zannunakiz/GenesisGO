import SwaggerParser from '@apidevtools/swagger-parser'
import { readFile } from 'node:fs/promises'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const webRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const repoRoot = resolve(webRoot, '..')
const specPath = join(webRoot, 'public', 'openapi.yaml')
const spec = await SwaggerParser.validate(specPath)

if (spec.openapi !== '3.1.0') {
   throw new Error(`Expected OpenAPI 3.1.0, received ${spec.openapi}`)
}

const documented = new Set()
for (const [path, item] of Object.entries(spec.paths)) {
   for (const method of Object.keys(item)) {
      if (['get', 'post', 'put', 'patch', 'delete', 'head', 'options'].includes(method)) {
         documented.add(`${method.toUpperCase()} ${path}`)
      }
   }
}

const registered = new Set()
const controlRoutes = await readFile(join(repoRoot, 'internal', 'httpapi', 'routes.go'), 'utf8')
for (const [, method, path] of controlRoutes.matchAll(/mux\.Handle\("([A-Z]+) ([^"]+)"\s*,/g)) {
   registered.add(`${method} ${path}`)
}
const healthRoutes = await readFile(join(repoRoot, 'internal', 'httpapi', 'health.go'), 'utf8')
for (const [, path] of healthRoutes.matchAll(/mux\.Handle\("(\/[^"]+)"\s*,/g)) {
   registered.add(`GET ${path}`)
}

const missing = [...registered].filter(route => !documented.has(route)).sort()
const stale = [...documented].filter(route => !registered.has(route)).sort()
if (missing.length || stale.length) {
   throw new Error([
      missing.length ? `Undocumented routes: ${missing.join(', ')}` : '',
      stale.length ? `Unregistered documented routes: ${stale.join(', ')}` : '',
   ].filter(Boolean).join('\n'))
}

const schemas = spec.components?.schemas ?? {}
for (const name of ['ErrorEnvelope', 'CreateRunRequest', 'Run', 'Report']) {
   if (!schemas[name]) throw new Error(`Missing representative schema: ${name}`)
}
for (const name of ['code', 'message', 'request_id']) {
   if (!schemas.ErrorEnvelope.required.includes(name)) {
      throw new Error(`ErrorEnvelope is missing required field ${name}`)
   }
}
if (!spec.paths['/api/v1/runs']?.post?.responses?.['201']) {
   throw new Error('Create-run success response (201) is missing')
}
if (!spec.paths['/api/v1/runs']?.post?.responses?.['400']) {
   throw new Error('Create-run validation response (400) is missing')
}

console.log(`OpenAPI ${spec.openapi} valid; ${registered.size} registered routes documented.`)
