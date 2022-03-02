import type { Knex } from "knex";

type RefsDeclaration = Readonly<
  Record<
    string,
    string | Readonly<{ type: "fk" | "m2m" | "expr"; [p: string]: any }>
  >
>;

type ColumnDeclaration<T = string> = T | RefsDeclaration;

type ColumnsDefinition<K extends string> = Readonly<Record<K, string>> &
  Iterable<string>;

type RefsDefinition<K extends string> = Readonly<Record<K, any>>;

type Ref<K extends string, T> = Readonly<
  Record<K, Readonly<{ type: T; [p: string]: any }>>
>;

type GetCols<T> = T extends string ? T : keyof GetColsFromObject<T>;

type GetColsFromObject<T> = {
  [P in keyof T as T[P] extends { type: "expr" } | string ? P : never]: T[P];
};

type GetColsFromTuple<T> = T extends [infer H, ...infer R]
  ? GetCols<H> | GetColsFromTuple<R>
  : never;

type GetRefs<T> = T extends object
  ? Extract<keyof GetRefsFromObject<T>, string>
  : never;

type GetRefsFromObject<T> = {
  [P in keyof T as T[P] extends { type: "expr" } | string ? never : P]: T[P];
};

type GetRefsFromTuple<T> = T extends [infer H, ...infer R]
  ? GetRefs<H> | GetRefsFromTuple<R>
  : never;

export type Expr<K extends string> = Ref<K, "expr">;

export type FK<K extends string> = Ref<K, "fk">;

export type M2M<K extends string> = Ref<K, "m2m">;

export type TableDefinition<C extends string, R extends string> = {
  with<CC extends ColumnDeclaration[]>(
    ...cc: [...CC]
  ): TableDefinition<C | GetColsFromTuple<CC>, R | GetRefsFromTuple<CC>>;

  pick<KK extends (C | R)[]>(
    ...cc: [...KK]
  ): TableDefinition<Extract<C, KK>, Extract<R, KK>>;

  omit<KK extends C | R>(
    ...kk: KK[]
  ): TableDefinition<Exclude<C, KK>, Exclude<R, KK>>;

  as(alias: string): TableDefinition<C, R>;
  named(tableName: string): TableDefinition<C, R>;
  quotes(quotes: string): TableDefinition<C, R>;
  separator(sep: string): TableDefinition<C, R>;

  readonly tableName: string;
  readonly cols: ColumnsDefinition<C>;
  readonly refs: RefsDefinition<R>;

  readonly options: Readonly<{
    quotes: string;
    separator: string;
    prefix: string;
  }>;
};

const createColsAndRefs = (
  tableName,
  colsDecl,
  { prefix, quotes: q, separator }
) => {
  const sel = [] as any[];
  const cols = {};
  const refs = {};

  cols.toString = () => sel.toString();
  cols[Symbol.iterator] = () => sel.values();

  const addItem = (newName, mapName, isExpr) => {
    const mapped = newName !== mapName || isExpr;
    const sep = prefix ? separator : "";
    const value = mapped
      ? `${q}${prefix}${sep}${newName}${q}`
      : `${q}${tableName}${q}.${q}${mapName}${q}`;
    cols[newName] = value;

    const aliased = isExpr
      ? `${mapName} as ${value}`
      : `${q}${tableName}${q}.${q}${mapName}${q} as ${q}${prefix}${sep}${newName}${q}`;
    sel.push(aliased);
  };

  colsDecl.forEach((decl) => {
    if (typeof decl === "string") addItem(decl, decl, false);
    else
      for (const k in decl) {
        const x = decl[k];
        if (typeof x === "string") addItem(k, x, false);
        else if (x.type === "expr") addItem(k, x.value, true);
        else refs[k] = x;
      }
  });

  return { cols, refs };
};

const merge = (fields, extraFields) => [
  ...new Set([...fields, ...extraFields]),
];

export function pick<T, K extends keyof T>(
  obj: T,
  keys: ReadonlyArray<K>
): Pick<T, K>;
export function pick(obj, keys) {
  const res = {};
  keys.forEach((k) => (k in obj ? (res[k] = obj[k]) : undefined));
  return res;
}

export function omit<T, K extends string>(
  obj: T,
  keys: ReadonlyArray<K>
): Omit<T, K>;
export function omit(obj, keys) {
  const omitKeys = new Set(keys);
  const res = {};
  for (const k in obj) if (!omitKeys.has(k)) res[k] = obj[k];
  return res;
}

const pick2 = (cols, refs, pickFields) => [
  pick(cols, pickFields),
  pick(refs, pickFields),
];

const omit2 = (cols, refs, omitFields) => [
  omit(cols, omitFields),
  omit(refs, omitFields),
];

export const tableFactory =
  (defaultOptions) =>
  <C extends ColumnDeclaration[]>(
    tableName: string,
    colsDecl: [...C],
    options?
  ) =>
    table(tableName, colsDecl, {
      ...defaultOptions,
      ...options,
    });

export const table = <C extends ColumnDeclaration>(
  tableName: string,
  colsDecl: ReadonlyArray<C>,
  options = {} as any
): TableDefinition<GetCols<C>, GetRefs<C>> => {
  // prettier-ignore
  const { quotes = "", separator = ":", prefix = tableName, ...restOptions } = options;
  options = { quotes, separator, prefix, ...restOptions };

  const { cols, refs } = createColsAndRefs(tableName, colsDecl, options);

  // prettier-ignore
  return {
    with: (...cc) => table(tableName, merge(colsDecl, cc), options),
    pick: (...kk) => table(tableName, pick2(cols, refs, kk), options),
    omit: (...kk) => table(tableName, omit2(cols, refs, kk), options),

    named: (tableName) => table(tableName, colsDecl, options),
    quotes: (quotes) => table(tableName, colsDecl, { ...options, quotes }),
    separator: separator => table(tableName, colsDecl, { ...options, separator }),
    as: (prefix) => table(tableName, colsDecl, { ...options, prefix }),

    cols,
    refs,
    options,
    tableName,
    toString: () => `${quotes}${tableName}${quotes}`
  } as any
};

export const expr = (value) =>
  ({
    type: "expr",
    value,
  } as const);

export const fk = (
  theirTable,
  ourCol = `${theirTable.tableName}Id`,
  theirCol = "id"
) =>
  ({
    type: "fk",
    theirTable,
    ourCol,
    theirCol,
  } as const);

export const m2m = (
  theirTable,
  viaTable,
  ourId = "id",
  viaOurId = "",
  theirId = "id",
  viaTheirId = `${theirTable.tableName}Id`
) =>
  ({
    type: "m2m",
    theirTable,
    viaTable,
    ourId,
    viaOurId,
    theirId,
    viaTheirId,
  } as const);

const bannedKeys = new Set(["__proto__", "prototype", "constructor"]);

const deepSet = (ctx, path, value, sep) =>
  path.split(sep).reduce((ctx, k, i, a) => {
    if (bannedKeys.has(k)) throw new Error("Found banned key: " + k);
    if (i === a.length - 1) return (ctx[k] = value);

    let nextCtx = ctx[k];
    if (!nextCtx) ctx[k] = nextCtx = isNaN(k) ? {} : [];

    return nextCtx;
  }, ctx);

export const resolve = (obj, sep: string) => {
  if (Array.isArray(obj)) return obj.map((x) => resolve(x, sep));
  if (typeof obj === "object") {
    const ctx = {};
    for (const k in obj) {
      deepSet(ctx, k, resolve(obj[k], sep), sep);
    }
    return ctx;
  }
  return obj;
};

const byId = (arr, getId = (x) => x.id, getV = (x) => x) =>
  Object.fromEntries(arr.map((x) => [getId(x), getV(x)]));

const selCol = ({ options: { prefix, separator } }, name) => {
  const sep = prefix ? separator : "";
  return `${prefix}${sep}${name}`;
};

// helper to get `table[col]` even if it's not in `table.cols`
const dotCol = (table, name) => `${table.tableName}.${name}`;

export const omitRefs = (obj, table) => omit(obj, Object.keys(table.refs));

export function createSelect(knex: Knex) {
  async function get_m2m({
    data,
    getId,
    ourTable,
    viaTable,
    theirTable,
    viaOurId = ourTable.tableName + "Id",
    theirId = "id",
    viaTheirId = theirTable.tableName + "Id",
  }) {
    theirTable = theirTable.as("");

    const ids = data.map(getId);

    // get their ids
    const theirIds = await knex
      .select(...viaTable.cols)
      .from(viaTable.tableName)
      .whereIn(dotCol(viaTable, viaOurId), ids);

    // build map {1: [1, 2, 3]}
    const ourIdsTheirIds = {};

    theirIds.forEach((row) => {
      const ourId = row[selCol(viaTable, viaOurId)];
      const theirIds = ourIdsTheirIds[ourId] || [];
      theirIds.push(row[selCol(viaTable, viaTheirId)]);
      ourIdsTheirIds[ourId] = theirIds;
    });

    // add fk's to the query and queue m2m's
    let modQ = (q) => q;

    const m2ms = [] as any[];

    const refsData = [
      {
        refs: theirTable.refs,
        info: {
          getObject: (x) => x,
          path: "",
          owner: theirTable,
        },
      },
    ];

    for (const { refs, info } of refsData) {
      for (const k in refs) {
        const x = refs[k];

        switch (x.type) {
          case "fk": {
            const _tt = x.theirTable;
            const name = _tt.tableName;
            const as = info.path ? info.path + ":" + name : name;
            const tt = _tt.as(as);

            const prev_modQ = modQ;
            modQ = (q) =>
              prev_modQ(q)
                .join(
                  tt.tableName,
                  dotCol(info.owner, x.ourCol),
                  dotCol(tt, x.theirCol)
                )
                .select(...tt.cols);

            refsData.push({
              refs: tt.refs,
              info: {
                getObject: (r) => info.getObject(r)[name],
                path: info.path ? info.path + ":" + name : name,
                owner: tt,
              },
            });

            break;
          }

          case "m2m": {
            const name = k;

            m2ms.push({
              ...x,
              key: name,
              getObject: info.getObject,
              getId: (r) => info.getObject(r)[x.ourId],
              path: info.path ? info.path + ":" + name : name,
              ourTable: info.owner,
              viaOurId: x.viaOurId || info.owner.tableName + "Id",
            });

            break;
          }
        }
      }
    }

    // get their data
    const q = knex
      .select(...theirTable.cols)
      .from(viaTable.tableName)
      .modify(modQ)
      .whereIn(dotCol(viaTable, viaOurId), ids)
      .join(
        theirTable.tableName,
        dotCol(viaTable, viaTheirId),
        dotCol(theirTable, theirId)
      )
      .groupBy(dotCol(theirTable, theirId));

    const theirData = resolve(await q, ":");

    // convert to lookup
    const theirDataById = byId(
      theirData,
      (x) => x[selCol(theirTable, theirId)]
    );

    // resolve m2m's
    for (const m2m of m2ms) {
      const data = await get_m2m({ ...m2m, data: theirData });

      theirData.forEach((r) => {
        m2m.getObject(r)[m2m.key] = data[m2m.getId(r)] || [];
      });
    }

    // resolve their ids
    const result = {};

    for (const k in ourIdsTheirIds) {
      const theirIds = ourIdsTheirIds[k];
      result[k] = theirIds.map((id) => theirDataById[id]);
    }

    return result;
  }

  return async (
    table: TableDefinition<any, any>,
    query = (q: Knex.QueryBuilder) => knex()
  ) => {
    table = table.as("");

    let modQ = (q) => q;

    const m2ms = [] as any[];

    const refsData = [
      {
        refs: table.refs,
        info: {
          getObject: (x) => x,
          path: "",
          owner: table,
        },
      },
    ];

    for (const { refs, info } of refsData) {
      for (const k in refs) {
        const x = refs[k];

        switch (x.type) {
          case "fk": {
            const _tt = x.theirTable;
            const name = _tt.tableName;
            const as = info.path ? info.path + ":" + name : name;
            const tt = _tt.as(as);

            const prev_modQ = modQ;
            modQ = (q) =>
              prev_modQ(q)
                .join(
                  tt.tableName,
                  dotCol(info.owner, x.ourCol),
                  dotCol(tt, x.theirCol)
                )
                .select(...tt.cols);

            refsData.push({
              refs: tt.refs,
              info: {
                getObject: (r) => info.getObject(r)[name],
                path: info.path ? info.path + ":" + name : name,
                owner: tt,
              },
            });

            break;
          }

          case "m2m": {
            const name = k;

            m2ms.push({
              ...x,
              key: name,
              getObject: info.getObject,
              getId: (r) => info.getObject(r)[x.ourId],
              path: info.path ? info.path + ":" + name : name,
              ourTable: info.owner,
              viaOurId: x.viaOurId || info.owner.tableName + "Id",
            });

            break;
          }
        }
      }
    }

    const q = knex(table.tableName)
      .select(...table.cols)
      .modify(modQ)
      .modify(query);

    const results = resolve(await q, ":");

    for (const m2m of m2ms) {
      const data = await get_m2m({ ...m2m, data: results });

      results.forEach((r) => {
        m2m.getObject(r)[m2m.key] = data[m2m.getId(r)] || [];
      });
    }

    return results;
  };
}

export const getFirst = <T = any>(p: Promise<ReadonlyArray<T>>) =>
  p.then((rows) => rows[0]);
