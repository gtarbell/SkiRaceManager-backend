import { APIGatewayProxyEventV2, APIGatewayProxyResultV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, DeleteCommand, PutCommand, QueryCommand, BatchGetCommand } from "@aws-sdk/lib-dynamodb";

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

type TeamScore = {
  gender: Gender;
  teamId: string;
  teamName: string;
  run1TotalSec: number | null;
  run2TotalSec: number | null;
  totalTimeSec: number | null;
  run1Contribs: { bib: number; racerName: string; timeSec: number }[];
  run2Contribs: { bib: number; racerName: string; timeSec: number }[];
  points: number;
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
const TEAMS = process.env.TEAMS_TABLE!;

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
  if (c === "va" || c.includes("alternate") || c.includes("alt")) return "Varsity Alternate";
  if (c === "v" || c.includes("varsity")) return "Varsity";
  return "Unknown";
}

function scoringClass(cls: string | undefined): RacerClass {
  const normalized = normalizeClass(cls);
  return normalized === "Varsity Alternate" ? "Varsity" : normalized;
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

async function getNonLeagueTeamIds(teamIds: string[]): Promise<Set<string>> {
  const unique = Array.from(new Set(teamIds.filter(Boolean)));
  const nonLeague = new Set<string>();
  const chunkSize = 100;
  for (let i = 0; i < unique.length; i += chunkSize) {
    const chunk = unique.slice(i, i + chunkSize);
    const res = await ddb.send(new BatchGetCommand({
      RequestItems: {
        [TEAMS]: {
          Keys: chunk.map(teamId => ({ teamId })),
          ProjectionExpression: "teamId, nonLeague",
        },
      },
    }));
    const items = res.Responses?.[TEAMS] ?? [];
    for (const item of items) {
      if (item?.nonLeague) nonLeague.add(String(item.teamId));
    }
  }
  return nonLeague;
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

async function saveResults(raceId: string, entries: ParsedEntry[], issues: string[], teamScores: TeamScore[]) {
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
    Item: { raceId, bib: 0, generatedAt: new Date().toISOString(), issues, teamScores },
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
  const teamScores = (summary?.teamScores as TeamScore[] | undefined) ?? [];
  return { entries, issues: (summary?.issues as string[] | undefined) ?? [], teamScores };
}

function buildGroups(entries: ParsedEntry[]) {
  const groups: { gender: Gender; class: RacerClass; entries: ParsedEntry[] }[] = [];
  const byKey = new Map<string, ParsedEntry[]>();
  for (const entry of entries) {
    const cls = scoringClass(entry.class);
    const key = `${entry.gender}|${cls}`;
    if (!byKey.has(key)) byKey.set(key, []);
    byKey.get(key)!.push({ ...entry, class: cls });
  }
  for (const [key, list] of byKey) {
    const [gender, cls] = key.split("|") as [Gender, RacerClass];
    const sorted = list.slice().sort((a, b) => b.totalPoints - a.totalPoints || a.bib - b.bib);
    groups.push({ gender, class: cls, entries: sorted });
  }
  const rank = (g: Gender, c: RacerClass) => {
    const isFemale = g === "Female";
    const isMale = g === "Male";
    const cls = scoringClass(c);
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

function bestThreeSum(times: number[]): number | null {
  if (times.length < 3) return null;
  const sorted = times.slice().sort((a, b) => a - b);
  return sorted[0] + sorted[1] + sorted[2];
}

function computeTeamScores(entries: ParsedEntry[]): TeamScore[] {
  const scores: TeamScore[] = [];
  const genders: Gender[] = ["Female", "Male"];
  for (const gender of genders) {
    // teamId -> {run1Times, run2Times, teamName, racers}
    const byTeam: Record<string, { run1: ParsedEntry[]; run2: ParsedEntry[]; teamName: string; racers: Set<string> }> = {};
    for (const e of entries) {
      if (e.class !== "Varsity" || e.gender !== gender) continue;
      if (!byTeam[e.teamId || ""]) {
        byTeam[e.teamId || ""] = { run1: [], run2: [], teamName: e.teamName, racers: new Set() };
      }
      const bucket = byTeam[e.teamId || ""];
      bucket.racers.add(e.racerId || String(e.bib));
      if (e.run1.status === 1 && typeof e.run1.timeSec === "number") bucket.run1.push(e);
      if (e.run2.status === 1 && typeof e.run2.timeSec === "number") bucket.run2.push(e);
      if (!bucket.teamName) bucket.teamName = e.teamName;
    }

    const eligible: TeamScore[] = [];
    for (const [teamId, data] of Object.entries(byTeam)) {
      if (!teamId) continue;
      const run1Finishers = data.run1
        .slice()
        .sort((a, b) => (a.run1.timeSec! - b.run1.timeSec!));
      const run2Finishers = data.run2
        .slice()
        .sort((a, b) => (a.run2.timeSec! - b.run2.timeSec!));
      const run1Contribs = run1Finishers.slice(0, 3).map(e => ({ bib: e.bib, racerName: e.racerName, timeSec: e.run1.timeSec! }));
      const run2Contribs = run2Finishers.slice(0, 3).map(e => ({ bib: e.bib, racerName: e.racerName, timeSec: e.run2.timeSec! }));
      const run1Total = run1Contribs.length === 3 ? run1Contribs.reduce((s, r) => s + r.timeSec, 0) : null;
      const run2Total = run2Contribs.length === 3 ? run2Contribs.reduce((s, r) => s + r.timeSec, 0) : null;
      const total = run1Total !== null && run2Total !== null ? run1Total + run2Total : null;
      eligible.push({
        gender,
        teamId,
        teamName: data.teamName,
        run1TotalSec: run1Total,
        run2TotalSec: run2Total,
        totalTimeSec: total,
        run1Contribs,
        run2Contribs,
        points: 0,
      });
    }

    const big = Number.MAX_SAFE_INTEGER;
    eligible.sort((a, b) => {
      const aT = a.totalTimeSec ?? big;
      const bT = b.totalTimeSec ?? big;
      return aT - bT || a.teamName.localeCompare(b.teamName);
    });

    let prevTime: number | undefined;
    let prevRank = 0;
    const teamCount = eligible.filter(t => byTeam[t.teamId]?.racers.size >= 3).length;
    let validSeen = 0;
    eligible.forEach((t) => {
      if (t.totalTimeSec === null) {
        t.points = 0;
        return;
      }
      validSeen += 1;
      const rank = (prevTime !== undefined && t.totalTimeSec === prevTime) ? prevRank : validSeen;
      const pts = Math.max(0, (teamCount * 2) - (rank - 1) * 2);
      t.points = pts;
      prevTime = t.totalTimeSec;
      prevRank = rank;
    });

    scores.push(...eligible);
  }
  return scores;
}

export const resultsRouter = async (e: APIGatewayProxyEventV2): Promise<APIGatewayProxyResultV2> => {
  const raceId = e.pathParameters?.["raceId"];
  if (!raceId) return { statusCode: 400, body: JSON.stringify({ error: "raceId required" }) };
  const method = e.requestContext.http.method;

  if (method === "GET") {
    const res = await loadResults(raceId);
    const nonLeagueTeamIds = await getNonLeagueTeamIds(
      res.entries.map(e => e.teamId || "")
    );
    const scoringEntries = res.entries.filter(e => !nonLeagueTeamIds.has(e.teamId || ""));
    const responseEntries = serializeEntries(res.entries);
    const groups = buildGroups(scoringEntries).map(g => ({ ...g, entries: serializeEntries(g.entries) }));
    return { statusCode: 200, body: JSON.stringify({ entries: responseEntries, issues: res.issues, groups, teamScores: res.teamScores }) };
  }

  if (method === "POST") {
    const body = JSON.parse(e.body || "{}");
    const xml = String(body.xml || "");
    if (!xml.trim()) return { statusCode: 400, body: JSON.stringify({ error: "xml required" }) };

    const startList = await getStartList(raceId);
    const byBib = new Map<number, StartListEntry>();
    startList.forEach(e => byBib.set(e.bib, e));
    const nonLeagueTeamIds = await getNonLeagueTeamIds(startList.map(e => e.teamId));

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
      const cls = sl.class;
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
      if (nonLeagueTeamIds.has(entry.teamId || "")) continue;
      const cls = scoringClass(entry.class);
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
      const cls = scoringClass(e.class);
      const gender = e.gender;
      const list = groupMap.get(`${gender}|${cls}`) || [];
      const updated = list.find(x => x.bib === e.bib) || e;
      return {
        ...updated,
        class: normalizeClass(e.class) as RacerClass,
        gender,
        totalPoints: (updated.run1Points || 0) + (updated.run2Points || 0),
      };
    }).sort((a, b) => b.totalPoints - a.totalPoints || a.bib - b.bib);

    const teamScores = computeTeamScores(finalEntries.filter(e => !nonLeagueTeamIds.has(e.teamId || "")));
    await saveResults(raceId, finalEntries, issues, teamScores);
    const responseEntries = serializeEntries(finalEntries);
    const groups = buildGroups(finalEntries.filter(e => !nonLeagueTeamIds.has(e.teamId || "")))
      .map(g => ({ ...g, entries: serializeEntries(g.entries) }));
    return { statusCode: 200, body: JSON.stringify({ entries: responseEntries, issues, groups, teamScores }) };
  }

  return { statusCode: 405, body: JSON.stringify({ error: "Method not allowed" }) };
};
