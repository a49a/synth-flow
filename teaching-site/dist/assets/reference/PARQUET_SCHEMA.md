# Parquet output schema

Without `output.schema`, SynthFlow infers a schema from the first accepted row.
Every later row and the published file use that same schema. A new field, a
non-null value for a column inferred as null, or a numeric value requiring a
lossy cast goes to the dead-letter file. A floating-point value is not silently
truncated to an integer. Integers too large to convert exactly to `float64` are
also rejected. Use an explicit schema when the first row has nulls or empty
arrays, or when fields can appear later.

`output.schema` describes the complete output object, including input fields,
`generated`, and `judge` if enabled. SynthFlow adds `_meta` automatically; do not
declare it yourself. This schema controls Parquet storage separately from
`generate.output_schema`, which validates the model response.

```yaml
output:
  format: parquet
  path: output/data.parquet
  schema:
    type: object
    required: [topic, generated]
    properties:
      topic: {type: string}
      count: {type: [integer, "null"]}
      tags:
        type: array
        items: {type: string}
      generated:
        type: object
        required: [answer]
        properties:
          answer: {type: string}
          confidence: {type: [number, "null"]}
```

This example accepts `count: null`, `tags: []`, and a missing `confidence` in the
first row, followed by populated values in subsequent rows. Optional properties
may be absent. To also allow an explicit JSON null, include `"null"` in `type`.
Missing optional values become nulls in Parquet. Array items can likewise be
nullable. Required properties must exist even when their type allows null.

Supported types are `string`, `boolean`, `integer` (signed 64-bit), `number`
(64-bit floating point), `null`, `object` with `properties`, and `array` with
`items`. Use `format: uint64` with `type: integer` for unsigned 64-bit IDs.
`required`, `additionalProperties: false`, `title`, and `description` are
supported. Object properties must be nonempty. Extra fields are always rejected;
`additionalProperties: true`, arbitrary unions, schema references, and other
JSON Schema keywords are rejected at configuration validation rather than
silently ignored. Keep richer response constraints in `generate.output_schema`.

The explicit schema is preserved even when a run accepts no rows, and on resume.
The current schema never changes midway through a run. Use a new output path
when changing the pipeline or schema.

## Checkpoint integrity and initialization

Every new checkpoint records BLAKE3 digests of the exact accepted and rejected
JSONL byte prefixes. Digests are maintained incrementally during writes, so a
checkpoint does not re-read earlier output. Resume checks both prefixes before
truncating uncommitted tails or replaying records. A same-length edit is detected
as well as a truncated file. Legacy manifests without digests remain readable;
their row structure and counts are verified and their next checkpoint adds
both digests. A digest cannot retroactively detect modifications made before a
legacy manifest was upgraded.

Parquet resume also checks every committed row against the recovered fixed
schema before truncating either data file or updating the manifest. Older
versions could accept values requiring lossy casts, such as an integer first
row followed by a fractional value. Such historical prefixes are now refused,
even when their digests match. All artifacts are preserved; use a new output
path to rerun with a suitable explicit schema. Verification streams one row at
a time and shares the prefix-hashing pass.

A new run atomically creates a complete manifest with `initializing: true`
before creating its empty data files, then clears the flag after syncing them.
Resume may recreate a missing data file only for that explicit initialization
state, with zero progress and empty digests. Existing nonempty files are refused
and preserved. Unknown artifacts and old empty/invalid manifests are never
removed automatically; inspect or move them aside manually before reusing the
output path. Temporary files abandoned before the atomic manifest creation can
be left in place safely because each has a unique name.
