import { APIGatewayProxyEventV2, APIGatewayProxyResultV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand, ScanCommand, GetCommand, PutCommand, DeleteCommand } from "@aws-sdk/lib-dynamodb";

type Gender = "Male" | "Female";
type RacerClass = "Varsity" | "Varsity Alternate" | "Jr Varsity" | "Provisional";

type StartListEntry = {
  raceId: string;
  racerId: string;
  racerName: string;
  teamId: string;
  teamName: string;
  gender: Gender;
  class: RacerClass;
  bib: number;
};
type StartListMeta = {
  teamsOrder: string[];
};

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));

const TEAMS = process.env.TEAMS_TABLE!;
const RACERS = process.env.RACERS_TABLE!;
const ROSTERS = process.env.ROSTERS_TABLE!;
const STARTLISTS = process.env.STARTLISTS_TABLE!;

const racingClassOrder: RacerClass[] = ["Varsity", "Varsity Alternate", "Jr Varsity", "Provisional"];

function shuffle<T>(arr: T[]): T[] {
  const a = arr.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

async function getTeams() {
  const scan = await ddb.send(new ScanCommand({ TableName: TEAMS }));
  const teams = scan.Items ?? [];
  const withRacers = await Promise.all(
    teams.map(async (t) => {
      const racers = await ddb.send(new QueryCommand({
        TableName: RACERS,
        IndexName: "byTeam",
        KeyConditionExpression: "teamId = :t",
        ExpressionAttributeValues: { ":t": t.teamId },
      }));
      return { ...t, racers: racers.Items ?? [] };
    })
  );
  return withRacers;
}

async function getRoster(raceId: string, teamId: string) {
  const res = await ddb.send(new QueryCommand({
    TableName: ROSTERS,
    KeyConditionExpression: "pk = :pk",
    ExpressionAttributeValues: { ":pk": `ROSTER#${raceId}#${teamId}` },
  }));
  return res.Items ?? [];
}

async function deleteExistingStartList(raceId: string) {
  const existing = await ddb.send(new QueryCommand({
    TableName: STARTLISTS,
    KeyConditionExpression: "raceId = :r",
    ExpressionAttributeValues: { ":r": raceId },
  }));
  for (const item of existing.Items ?? []) {
    await ddb.send(new DeleteCommand({
      TableName: STARTLISTS,
      Key: { raceId, bib: item.bib },
    }));
  }
}

async function getExcludedBibs(raceId: string): Promise<number[]> {
  const res = await ddb.send(new GetCommand({
    TableName: STARTLISTS,
    Key: { raceId, bib: 0 },
  }));
  return (res.Item?.excludedBibs as number[] | undefined) ?? [];
}

async function getStartListData(raceId: string): Promise<{ entries: StartListEntry[]; meta?: StartListMeta; excludedBibs: number[] }> {
  const res = await ddb.send(new QueryCommand({
    TableName: STARTLISTS,
    KeyConditionExpression: "raceId = :r",
    ExpressionAttributeValues: { ":r": raceId },
  }));
  const metaItem = (res.Items ?? []).find(i => i.bib === 0);
  const meta: StartListMeta | undefined = metaItem?.meta as any;
  const excludedBibs = (metaItem?.excludedBibs as number[] | undefined) ?? [];
  const entries: StartListEntry[] = (res.Items ?? [])
    .filter(i => i.bib !== 0)
    .map(i => ({
      raceId,
      racerId: i.racerId,
      racerName: i.racerName,
      teamId: i.teamId,
      teamName: i.teamName,
      gender: i.gender,
      class: i.class,
      bib: i.bib,
    }))
    .sort((a, b) => a.bib - b.bib);
  return { entries, meta, excludedBibs };
}

async function getMeta(raceId: string): Promise<{ excludedBibs: number[]; meta?: StartListMeta } | null> {
  const res = await ddb.send(new GetCommand({
    TableName: STARTLISTS,
    Key: { raceId, bib: 0 },
  }));
  if (!res.Item) return null;
  return { excludedBibs: (res.Item.excludedBibs as number[] | undefined) ?? [], meta: res.Item.meta as StartListMeta | undefined };
}

async function putExcludedBibs(raceId: string, excludedBibs: number[]) {
  await ddb.send(new PutCommand({
    TableName: STARTLISTS,
    Item: { raceId, bib: 0, excludedBibs },
  }));
}

function nextAvailableBib(start: number, exclude: Set<number>, count: number): number[] {
  const res: number[] = [];
  let num = start;
  while (res.length < count) {
    if (!exclude.has(num)) res.push(num);
    num++;
  }
  return res;
}

export const startlistRouter = async (e: APIGatewayProxyEventV2): Promise<APIGatewayProxyResultV2> => {
  const method = e.requestContext.http.method;
  const raceId = e.pathParameters?.["raceId"];
  if (!raceId) return { statusCode: 400, body: JSON.stringify({ error: "raceId required" }) };

  if (method === "GET" && e.rawPath.endsWith("/excluded")) {
    const excluded = await getExcludedBibs(raceId);
    return { statusCode: 200, body: JSON.stringify(excluded) };
  }

  if (method === "POST" && e.rawPath.endsWith("/excluded")) {
    const body = JSON.parse(e.body || "{}");
    const excluded = Array.isArray(body.excludedBibs) ? body.excludedBibs.filter((n: any) => Number.isFinite(n)) : [];
    await putExcludedBibs(raceId, excluded);
    return { statusCode: 200, body: JSON.stringify(excluded) };
  }

  if (method === "GET") {
    const res = await getStartListData(raceId);
    return { statusCode: 200, body: JSON.stringify({ entries: res.entries, meta: res.meta }) };
  }

  if (method === "POST" && e.rawPath.endsWith("/copy")) {
    const body = JSON.parse(e.body || "{}");
    const fromRaceId = String(body.fromRaceId || "");
    if (!fromRaceId) return { statusCode: 400, body: JSON.stringify({ error: "fromRaceId required" }) };
    if (fromRaceId === raceId) return { statusCode: 400, body: JSON.stringify({ error: "Choose a different race to copy from" }) };

    const source = await getStartListData(fromRaceId);
    if (!source.entries.length && (!source.meta || source.excludedBibs.length === 0)) {
      return { statusCode: 404, body: JSON.stringify({ error: "Source start list not found" }) };
    }

    await deleteExistingStartList(raceId);

    await ddb.send(new PutCommand({
      TableName: STARTLISTS,
      Item: {
        raceId,
        bib: 0,
        excludedBibs: source.excludedBibs,
        ...(source.meta ? { meta: source.meta } : {}),
      },
    }));

    for (const entry of source.entries) {
      await ddb.send(new PutCommand({
        TableName: STARTLISTS,
        Item: {
          raceId,
          bib: entry.bib,
          racerId: entry.racerId,
          racerName: entry.racerName,
          teamId: entry.teamId,
          teamName: entry.teamName,
          gender: entry.gender,
          class: entry.class,
        },
      }));
    }

    return { statusCode: 200, body: JSON.stringify({ entries: source.entries, meta: source.meta }) };
  }

  if (method === "POST" && e.rawPath.endsWith("/generate")) {
    const body = JSON.parse(e.body || "{}") as { excludedBibs?: number[] };
    const excludesFromBody = Array.isArray(body.excludedBibs) ? body.excludedBibs.filter(n => Number.isFinite(n)) : undefined;
    const existingMeta = await getMeta(raceId);
    const excludeList = excludesFromBody ?? existingMeta?.excludedBibs ?? await getExcludedBibs(raceId);
    await putExcludedBibs(raceId, excludeList);
    const excludedSet = new Set<number>(excludeList);

    const teams = await getTeams() as any[];
    const allTeamIds = teams.map(t => (t as any).teamId as string);
    const existingOrder = (existingMeta?.meta?.teamsOrder ?? []).filter(id => allTeamIds.includes(id));
    const remainingTeams = allTeamIds.filter(id => !existingOrder.includes(id));
    const baseTeamOrder = existingOrder.length
      ? [...existingOrder, ...shuffle(remainingTeams)]
      : shuffle(allTeamIds.slice());
    const rosterByTeam: Record<string, any[]> = {};
    for (const tid of allTeamIds) {
      rosterByTeam[tid] = await getRoster(raceId, tid);
    }

    const makeGenderList = (gender: Gender): { entries: StartListEntry[]; teamOrder: string[] } => {
      const result: StartListEntry[] = [];
      const teamOrderAccumulator: string[] = [];
      for (const cls of racingClassOrder) {
        let maxPos = 0;
        for (const tid of allTeamIds) {
          const posMax = (rosterByTeam[tid] || [])
            .filter(e => e.gender === gender && e.class === cls)
            .reduce((m, e) => Math.max(m, e.startOrder ?? 0), 0);
          maxPos = Math.max(maxPos, posMax);
        }
        if (maxPos === 0) continue;
        const randomizedTeams = baseTeamOrder;
        teamOrderAccumulator.push(...randomizedTeams);
        for (let pos = 1; pos <= maxPos; pos++) {
          const forward = (pos % 2) === 1;
          const order = forward ? randomizedTeams : randomizedTeams.slice().reverse();
          for (const tid of order) {
            const entry = (rosterByTeam[tid] || []).find(
              (e: any) => e.gender === gender && e.class === cls && e.startOrder === pos
            );
            if (!entry) continue;
            const team = teams.find(t => t.teamId === tid);
            const racer = team?.racers.find((r: any) => r.racerId === entry.racerId);
            if (!racer || !team) continue;
            result.push({
              raceId,
              racerId: entry.racerId,
              racerName: racer.name,
              teamId: team.teamId,
              teamName: team.name,
              gender,
              class: cls,
              bib: 0,
            });
          }
        }
      }
      return { entries: result, teamOrder: Array.from(new Set(teamOrderAccumulator)) };
    };

    const womenRes = makeGenderList("Female");
    const menRes = makeGenderList("Male");
    const women = womenRes.entries;
    const men = menRes.entries;

    const womenBibs = nextAvailableBib(1, excludedSet, women.length);
    women.forEach((s, i) => s.bib = womenBibs[i]);

    const menBibs = nextAvailableBib(100, excludedSet, men.length);
    men.forEach((s, i) => s.bib = menBibs[i]);

    const full = [...women, ...men];

    await deleteExistingStartList(raceId);
    await putExcludedBibs(raceId, excludeList);
    const meta: StartListMeta = { teamsOrder: Array.from(new Set([...womenRes.teamOrder, ...menRes.teamOrder])) };
    await ddb.send(new PutCommand({
      TableName: STARTLISTS,
      Item: { raceId, bib: 0, excludedBibs: excludeList, meta },
    }));
    for (const entry of full) {
      await ddb.send(new PutCommand({
        TableName: STARTLISTS,
        Item: {
          raceId,
          bib: entry.bib,
          racerId: entry.racerId,
          racerName: entry.racerName,
          teamId: entry.teamId,
          teamName: entry.teamName,
          gender: entry.gender,
          class: entry.class,
        },
      }));
    }

    return { statusCode: 200, body: JSON.stringify({ entries: full, meta }) };
  }

  return { statusCode: 404, body: JSON.stringify({ error: "Not found" }) };
};
