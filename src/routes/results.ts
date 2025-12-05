import { APIGatewayProxyEventV2, APIGatewayProxyResultV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, DeleteCommand, PutCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";

type Gender = "Male" | "Female" | "Unknown";
type RacerClass = "Varsity" | "Varsity Alternate" | "Jr Varsity" | "Provisional" | "Unknown";

type RunInfo = {
  status: number;
  timeSec?: number;
};

type ParsedEntry = {
  raceId: string;
  bib: number;
  racerId?: string;
  racerName: string;
  teamId?: string;
  teamName: string;
  gender: Gender;
  class: RacerClass;
  run1: RunInfo;
  run2: RunInfo;
  run1Points: number;
  run2Points: number;
  totalPoints: number;
  issues?: string[];
};

type StartListEntry = {
  racerId: string;
  racerName: string;
  teamId: string;
  teamName: string;
  gender: Gender;
  class: RacerClass;
  bib: number;
};

const ladder = [100, 80, 60, 50, 45, 40, 36, 32, 29, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1];

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const STARTLISTS = process.env.STARTLISTS_TABLE!;
const RESULTS = process.env.RESULTS_TABLE!;

function normalizeName(name: string): string {
  const raw = name.trim();
  const lastCommaIdx = raw.indexOf(",");
  const flipped = lastCommaIdx >= 0 ? `${raw.slice(lastCommaIdx + 1)} ${raw.slice(0, lastCommaIdx)}` : raw;
  return flipped.replace(/[^a-z0-9\s]/gi, " ").toLowerCase().replace(/\s+/g, " ").trim();
}

function normalizeClass(cls: string | undefined): RacerClass {
  const c = (cls || "").toLowerCase();
  if (c === "jv" || c.includes("jv") || c.includes("jr")) return "Jr Varsity";
  if (c.startsWith("p")) return "Provisional";
  if (c === "v" || c === "va" || c.includes("varsity")) return "Varsity";
  return "Unknown";
}

function normalizeGender(g: string | undefined): Gender {
  if (!g) return "Unknown";
  const val = g.toLowerCase();
  if (val.startsWith("m")) return "Male";
  if (val.startsWith("f") || val.includes("lad")) return "Female";
  return "Unknown";
}

function parseRun(block: string | undefined): RunInfo {
  if (!block) return { status: 0 };
  const status = Number(block.match(/<Status>([^<]+)<\/Status>/)?.[1] || 0);
  const microStart = Number(block.match(/<MicroStart>([^<]+)<\/MicroStart>/)?.[1] || 0);
  const microFinish = Number(block.match(/<MicroFinish>([^<]+)<\/MicroFinish>/)?.[1] || 0);
  const hasTimes = microStart > 0 && microFinish > 0;
  if (!hasTimes) return { status: 0 };
  if (status !== 1) return { status };
  const finished = microFinish > microStart;
  const timeSec = finished ? (microFinish - microStart) / 1_000_000 : undefined;
  return finished ? { status, timeSec } : { status: 0 };
}

async function getStartList(raceId: string): Promise<StartListEntry[]> {
  const res = await ddb.send(new QueryCommand({
    TableName: STARTLISTS,
    KeyConditionExpression: "raceId = :r",
    ExpressionAttributeValues: { ":r": raceId },
  }));
  return (res.Items ?? [])
    .filter(i => typeof i.bib === "number" && i.bib > 0)
    .map(i => ({
      racerId: String(i.racerId),
      racerName: String(i.racerName),
      teamId: String(i.teamId),
      teamName: String(i.teamName),
      gender: normalizeGender(String(i.gender)),
      class: normalizeClass(String(i.class)),
      bib: Number(i.bib),
    }));
}

function parseComps(xml: string, fallbackGender: Gender): ParsedEntry[] {
  const comps: ParsedEntry[] = [];
  const compRegex = /<Comp>[\s\S]*?<\/Comp>/g;
  for (const match of xml.matchAll(compRegex)) {
    const block = match[0];
    const bib = Number(block.match(/<Bib>([^<]+)<\/Bib>/)?.[1] || 0);
    if (!Number.isFinite(bib) || bib <= 0) continue;
    const name = (block.match(/<Name>([^<]+)<\/Name>/)?.[1] || "").trim();
    const team = (block.match(/<Team>([^<]+)<\/Team>/)?.[1] || "").trim();
    const compClass = normalizeClass(block.match(/<CompClass>([^<]+)<\/CompClass>/)?.[1]);
    const run1Block = block.match(/<Time1>([\s\S]*?)<\/Time1>/)?.[1];
    const run2Block = block.match(/<Time2>([\s\S]*?)<\/Time2>/)?.[1];
    comps.push({
      raceId: "",
      bib,
      racerName: name,
      teamName: team,
      gender: fallbackGender,
      class: compClass,
      run1: parseRun(run1Block),
      run2: parseRun(run2Block),
      run1Points: 0,
      run2Points: 0,
      totalPoints: 0,
    });
  }
  return comps;
}

function competitionPoints(finishers: ParsedEntry[], runKey: "run1" | "run2") {
  const list = finishers
    .filter(e => e[runKey].status === 1 && typeof e[runKey].timeSec === "number")
    .sort((a, b) => (a[runKey].timeSec! - b[runKey].timeSec!));

  let prevTime: number | undefined;
  let prevRank = 0;
  list.forEach((entry, idx) => {
    const time = entry[runKey].timeSec!;
    const rank = (prevTime !== undefined && time === prevTime) ? prevRank : idx + 1;
    const points = ladder[rank - 1] ?? 0;
    if (runKey === "run1") entry.run1Points = points;
    else entry.run2Points = points;
    prevTime = time;
    prevRank = rank;
  });
}

async function saveResults(raceId: string, entries: ParsedEntry[], issues: string[]) {
  const existing = await ddb.send(new QueryCommand({
    TableName: RESULTS,
    KeyConditionExpression: "raceId = :r",
    ExpressionAttributeValues: { ":r": raceId },
  }));
  for (const item of existing.Items ?? []) {
    await ddb.send(new DeleteCommand({ TableName: RESULTS, Key: { raceId, bib: item.bib } }));
  }

  await ddb.send(new PutCommand({
    TableName: RESULTS,
    Item: { raceId, bib: 0, generatedAt: new Date().toISOString(), issues },
  }));

  for (const e of entries) {
    await ddb.send(new PutCommand({
      TableName: RESULTS,
      Item: {
        raceId,
        bib: e.bib,
        racerId: e.racerId,
        racerName: e.racerName,
        teamId: e.teamId,
        teamName: e.teamName,
        gender: e.gender,
        class: e.class,
        run1Status: e.run1.status,
        run2Status: e.run2.status,
        run1TimeSec: e.run1.timeSec,
        run2TimeSec: e.run2.timeSec,
        run1Points: e.run1Points,
        run2Points: e.run2Points,
        totalPoints: e.totalPoints,
      },
    }));
  }
}

async function loadResults(raceId: string) {
  const res = await ddb.send(new QueryCommand({
    TableName: RESULTS,
    KeyConditionExpression: "raceId = :r",
    ExpressionAttributeValues: { ":r": raceId },
  }));
  const items = res.Items ?? [];
  const summary = items.find(i => i.bib === 0);
  const entries = items
    .filter(i => i.bib !== 0)
    .map(i => ({
      raceId,
      bib: Number(i.bib),
      racerId: i.racerId as string | undefined,
      racerName: String(i.racerName ?? ""),
      teamId: i.teamId as string | undefined,
      teamName: String(i.teamName ?? ""),
      gender: normalizeGender(i.gender as string | undefined),
      class: normalizeClass(i.class as string | undefined),
      run1: { status: Number(i.run1Status ?? 0), timeSec: typeof i.run1TimeSec === "number" ? i.run1TimeSec : undefined },
      run2: { status: Number(i.run2Status ?? 0), timeSec: typeof i.run2TimeSec === "number" ? i.run2TimeSec : undefined },
      run1Points: Number(i.run1Points ?? 0),
      run2Points: Number(i.run2Points ?? 0),
      totalPoints: Number(i.totalPoints ?? 0),
    }))
    .sort((a, b) => b.totalPoints - a.totalPoints || a.bib - b.bib);
  return { entries, issues: (summary?.issues as string[] | undefined) ?? [] };
}

function buildGroups(entries: ParsedEntry[]) {
  const groups: { gender: Gender; class: RacerClass; entries: ParsedEntry[] }[] = [];
  const byKey = new Map<string, ParsedEntry[]>();
  for (const entry of entries) {
    const key = `${entry.gender}|${entry.class}`;
    if (!byKey.has(key)) byKey.set(key, []);
    byKey.get(key)!.push(entry);
  }
  for (const [key, list] of byKey) {
    const [gender, cls] = key.split("|") as [Gender, RacerClass];
    const sorted = list.slice().sort((a, b) => b.totalPoints - a.totalPoints || a.bib - b.bib);
    groups.push({ gender, class: cls, entries: sorted });
  }
  const rank = (g: Gender, c: RacerClass) => {
    const isFemale = g === "Female";
    const isMale = g === "Male";
    const cls = c === "Varsity Alternate" ? "Varsity" : c;
    if (isFemale && cls === "Varsity") return 1;
    if (isMale && cls === "Varsity") return 2;
    if (isFemale && cls === "Jr Varsity") return 3;
    if (isMale && cls === "Jr Varsity") return 4;
    if (isFemale && cls === "Provisional") return 5;
    if (isMale && cls === "Provisional") return 6;
    return 999;
  };
  groups.sort((a, b) => rank(a.gender, a.class) - rank(b.gender, b.class));
  return groups;
}

function serializeEntries(entries: ParsedEntry[]) {
  return entries.map(e => ({
    raceId: e.raceId,
    bib: e.bib,
    racerId: e.racerId,
    racerName: e.racerName,
    teamId: e.teamId,
    teamName: e.teamName,
    gender: e.gender,
    class: e.class,
    run1Status: e.run1.status,
    run2Status: e.run2.status,
    run1TimeSec: e.run1.timeSec,
    run2TimeSec: e.run2.timeSec,
    run1Points: e.run1Points,
    run2Points: e.run2Points,
    totalPoints: e.totalPoints,
  }));
}

export const resultsRouter = async (e: APIGatewayProxyEventV2): Promise<APIGatewayProxyResultV2> => {
  const raceId = e.pathParameters?.["raceId"];
  if (!raceId) return { statusCode: 400, body: JSON.stringify({ error: "raceId required" }) };
  const method = e.requestContext.http.method;

  if (method === "GET") {
    const res = await loadResults(raceId);
    const responseEntries = serializeEntries(res.entries);
    const groups = buildGroups(res.entries).map(g => ({ ...g, entries: serializeEntries(g.entries) }));
    return { statusCode: 200, body: JSON.stringify({ entries: responseEntries, issues: res.issues, groups }) };
  }

  if (method === "POST") {
    const body = JSON.parse(e.body || "{}");
    const xml = String(body.xml || "");
    if (!xml.trim()) return { statusCode: 400, body: JSON.stringify({ error: "xml required" }) };

    const startList = await getStartList(raceId);
    const byBib = new Map<number, StartListEntry>();
    startList.forEach(e => byBib.set(e.bib, e));

    const fallbackGender = normalizeGender(xml.match(/<CurrentSex>([^<]+)<\/CurrentSex>/)?.[1]);
    const parsed = parseComps(xml, fallbackGender);
    const issues: string[] = [];

    const merged: ParsedEntry[] = parsed.map(p => {
      const sl = byBib.get(p.bib);
      if (!sl) {
        issues.push(`Bib ${p.bib} not found in start list (file shows ${p.racerName})`);
        return { ...p, raceId, gender: p.gender, class: p.class };
      }
      const normFile = normalizeName(p.racerName);
      const normStart = normalizeName(sl.racerName);
      if (normFile && normStart && normFile !== normStart) {
        issues.push(`Bib ${p.bib} name mismatch: file "${p.racerName}" vs start list "${sl.racerName}"`);
      }
      const cls = sl.class === "Varsity" || sl.class === "Varsity Alternate" ? "Varsity" : sl.class;
      return {
        ...p,
        raceId,
        racerId: sl.racerId,
        racerName: sl.racerName,
        teamId: sl.teamId,
        teamName: sl.teamName,
        gender: sl.gender ?? p.gender,
        class: cls as RacerClass,
      };
    });

    const groupMap = new Map<string, ParsedEntry[]>();
    for (const entry of merged) {
      const baseClass = entry.class === "Varsity Alternate" ? "Varsity" : normalizeClass(entry.class);
      const cls = baseClass === "Unknown" ? "Unknown" : baseClass;
      const g = entry.gender;
      const key = `${g}|${cls}`;
      if (!groupMap.has(key)) groupMap.set(key, []);
      const arr = groupMap.get(key)!;
      arr.push({ ...entry, class: cls as RacerClass });
    }

    for (const [, list] of groupMap) {
      competitionPoints(list, "run1");
      competitionPoints(list, "run2");
    }

    const finalEntries = merged.map(e => {
      const baseClass = e.class === "Varsity Alternate" ? "Varsity" : normalizeClass(e.class);
      const cls = baseClass === "Unknown" ? "Unknown" : baseClass;
      const gender = e.gender;
      const list = groupMap.get(`${gender}|${cls}`) || [];
      const updated = list.find(x => x.bib === e.bib) || e;
      return {
        ...updated,
        class: cls as RacerClass,
        gender,
        totalPoints: (updated.run1Points || 0) + (updated.run2Points || 0),
      };
    }).sort((a, b) => b.totalPoints - a.totalPoints || a.bib - b.bib);

    await saveResults(raceId, finalEntries, issues);
    const responseEntries = serializeEntries(finalEntries);
    const groups = buildGroups(finalEntries).map(g => ({ ...g, entries: serializeEntries(g.entries) }));
    return { statusCode: 200, body: JSON.stringify({ entries: responseEntries, issues, groups }) };
  }

  return { statusCode: 405, body: JSON.stringify({ error: "Method not allowed" }) };
};
