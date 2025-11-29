// backend/scripts/seed.ts
/* eslint-disable no-console */
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";

// ======== CONFIG ========
const REGION = process.env.AWS_REGION || "us-east-2";
const TEAMS_TABLE = process.env.TEAMS_TABLE || "Teams";
const RACERS_TABLE = process.env.RACERS_TABLE || "Racers";
const RACES_TABLE  = process.env.RACES_TABLE  || "Races";

// ======== AWS INIT ========
const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({ region: REGION }));

// ======== DATA (your mock data) ========
// Minimal types
type Gender = "Male" | "Female";
type RacerClass = "Varsity" | "Varsity Alternate" | "Jr Varsity" | "Provisional" | "DNS";

type User = { id: string; name: string; role: "ADMIN" | "COACH"; teamIds: string[]; };
type Racer = { id: string; name: string; gender: Gender; class: RacerClass; teamId: string; };
type Team = { id: string; name: string; coachUserIds: string[]; racers: Racer[]; };
type Race = { id: string; name: string; location: string; date: string; type: "Slalom" | "Giant Slalom" };

// ---- Paste from your mock API ----
const users: User[] = [
  { id: "u1", name: "Geddy Admin", role: "ADMIN", teamIds: [] },
  { id: "u2", name: "Coach Josh", role: "COACH", teamIds: ["t4"] },
  { id: "u3", name: "Brad", role: "ADMIN", teamIds: [] },
  { id: "u4", name: "Eastside Coach", role: "COACH", teamIds: ["t2", "t3", "t1"] },
];

let teams: Team[] = [
  {
    id: "t4",
    name: "Sandy High School",
    coachUserIds: ["u2"],
    racers: [
      { id: "r100", name: "Ansel Ofstie", gender: "Male", class: "Varsity", teamId: "t4" },
      { id: "r101", name: "Mario Heckel", gender: "Male", class: "Varsity", teamId: "t4" },
      { id: "r102", name: "Dylan Brown", gender: "Male", class: "Varsity", teamId: "t4" },
      { id: "r103", name: "Grant Messinger", gender: "Male", class: "Varsity", teamId: "t4" },
      { id: "r104", name: "Ethan Van Hee", gender: "Male", class: "Varsity", teamId: "t4" },
      { id: "r105", name: "Beck Schreiner", gender: "Male", class: "Varsity Alternate", teamId: "t4" },
      { id: "r106", name: "Kai Muntz", gender: "Male", class: "Jr Varsity", teamId: "t4" },
      { id: "r107", name: "Max Kocubinski", gender: "Male", class: "Jr Varsity", teamId: "t4" },
      { id: "r108", name: "Hayden Ferschweiler", gender: "Male", class: "Jr Varsity", teamId: "t4" },
      { id: "r109", name: "Finley Lafayette", gender: "Male", class: "Jr Varsity", teamId: "t4" },
      { id: "r110", name: "Ben Hohl", gender: "Male", class: "Jr Varsity", teamId: "t4" },
      { id: "r111", name: "Jackson Mulick", gender: "Male", class: "Jr Varsity", teamId: "t4" },
      { id: "r112", name: "Jameson Stone", gender: "Male", class: "Jr Varsity", teamId: "t4" },
      { id: "r113", name: "Noah Lowery", gender: "Male", class: "Jr Varsity", teamId: "t4" },
      { id: "r114", name: "Henry Bird", gender: "Male", class: "Jr Varsity", teamId: "t4" },
      { id: "r115", name: "Ben Leiblein", gender: "Male", class: "Jr Varsity", teamId: "t4" },
      { id: "r116", name: "Coen Fleming-Harris", gender: "Male", class: "Jr Varsity", teamId: "t4" },

      // NOTE: The next racers belong to teamId "t5" (girls) even though they're listed here.
      { id: "r200", name: "Anika Wipper", gender: "Female", class: "Varsity", teamId: "t5" },
      { id: "r201", name: "Wallace Hamalanien", gender: "Female", class: "Varsity", teamId: "t5" },
      { id: "r202", name: "Anna Nguyen", gender: "Female", class: "Varsity", teamId: "t5" },
      { id: "r203", name: "Brynn Fleming-Harris", gender: "Female", class: "Varsity", teamId: "t5" },
      { id: "r204", name: "Hannah Ban", gender: "Female", class: "Varsity", teamId: "t5" },
      { id: "r205", name: "Keegan Deters", gender: "Female", class: "Varsity Alternate", teamId: "t5" },
      { id: "r206", name: "Chella Houston", gender: "Female", class: "Jr Varsity", teamId: "t5" },
      { id: "r207", name: "Brighton Wilson", gender: "Female", class: "Jr Varsity", teamId: "t5" },
      { id: "r208", name: "Addison Kolibaba", gender: "Female", class: "Jr Varsity", teamId: "t5" },
      { id: "r209", name: "Leah Shaw", gender: "Female", class: "Jr Varsity", teamId: "t5" },
      { id: "r210", name: "Montana Tarbell", gender: "Female", class: "Jr Varsity", teamId: "t5" },
      { id: "r211", name: "Ella Nguyen", gender: "Female", class: "Jr Varsity", teamId: "t5" },
      { id: "r212", name: "Athea Wehrung", gender: "Female", class: "Jr Varsity", teamId: "t5" },
      { id: "r213", name: "Rory Mason", gender: "Female", class: "Jr Varsity", teamId: "t5" },
      { id: "r214", name: "Payton Haney", gender: "Female", class: "Jr Varsity", teamId: "t5" },
      { id: "r215", name: "Josephine Bird", gender: "Female", class: "Jr Varsity", teamId: "t5" },
      { id: "r216", name: "Wren Schreiner", gender: "Female", class: "Provisional", teamId: "t5" },
    ],
  },
  {
    id: "t2",
    name: "Cleveland HS",
    coachUserIds: ["u4"],
    racers: [
      { id: "r3", name: "Riley Kim",  gender: "Female", class: "Provisional", teamId: "t2" },
      { id: "r4", name: "Morgan Fox", gender: "Female", class: "Varsity",     teamId: "t2" },
      { id: "r5", name: "Drew Park",  gender: "Male",   class: "Varsity",     teamId: "t2" },
    ],
  },
  { id: "t3", name: "Grant HS", coachUserIds: ["u4"], racers: [] },
  {
    id: "t1",
    name: "Franklin HS",
    coachUserIds: ["u4"],
    racers: [
      { id: "r3", name: "Isa Halle",  gender: "Female", class: "Varsity", teamId: "t1" },
      { id: "r4", name: "Cleo Craig", gender: "Female", class: "Varsity", teamId: "t1" }
    ],
  },
];

const races: Race[] = [
  { id: "race1", name: "Kelsey Race", location: "Meadows (Stadium)",  date: "2026-01-02", type: "Giant Slalom" },
  { id: "race2", name: "SL 1",        location: "Anthony Lakes",       date: "2026-01-10", type: "Slalom" },
  { id: "race3", name: "GS 1",        location: "Ski Bowl (MT Hood Lane)", date: "2026-01-19", type: "Giant Slalom" },
  { id: "race4", name: "SL 2",        location: "Ski Bowl (Challenger)",   date: "2026-01-30", type: "Slalom" },
  { id: "race5", name: "GS 2",        location: "Meadows (Middle Fork)",   date: "2026-02-08", type: "Giant Slalom" },
  { id: "race6", name: "GS 3",        location: "Meadows (Middle Fork)",   date: "2026-02-08", type: "Giant Slalom" },
  { id: "race7", name: "SL 3",        location: "Cooper Spur",             date: "2026-02-20", type: "Slalom" },
];

// ======== NORMALIZATION ========

// Build a map of teams from supplied list
const teamMap = new Map<string, { id: string; name: string; coachUserIds: string[] }>();
for (const t of teams) {
  teamMap.set(t.id, { id: t.id, name: t.name, coachUserIds: t.coachUserIds || [] });
}

// If any racer refers to a team that doesn’t exist, auto-create it
function ensureTeamExists(teamId: string) {
  if (teamMap.has(teamId)) return;
  // Heuristic for t5 given your data:
  const autoName = teamId === "t5" ? "Sandy High School (Girls)" : `Team ${teamId}`;
  console.warn(`→ Auto-creating missing team "${teamId}" as "${autoName}"`);
  teamMap.set(teamId, { id: teamId, name: autoName, coachUserIds: [] });
}

// Flatten all racers, honoring each racer's own teamId (not the parent)
const allRacers: Racer[] = [];
for (const t of teams) {
  for (const r of t.racers || []) {
    const tid = r.teamId || t.id;
    ensureTeamExists(tid);
    allRacers.push({ ...r, teamId: tid });
  }
}

// De-duplicate racer IDs (you reuse r3 & r4 on different teams)
const seenRacerIds = new Set<string>();
const dedupedRacers: Racer[] = allRacers.map((r) => {
  if (!seenRacerIds.has(r.id)) {
    seenRacerIds.add(r.id);
    return r;
  }
  const newId = `${r.id}_${r.teamId}`;
  console.warn(`→ Duplicate racerId "${r.id}" detected; renaming to "${newId}" for team ${r.teamId}`);
  return { ...r, id: newId };
});

// Final teams array = teamMap values
const finalTeams = Array.from(teamMap.values());

// ======== WRITE HELPERS ========
async function putTeam(t: { id: string; name: string; coachUserIds: string[] }) {
  await ddb.send(
    new PutCommand({
      TableName: TEAMS_TABLE,
      Item: { teamId: t.id, name: t.name, coachUserIds: t.coachUserIds },
    })
  );
}

async function putRacer(r: Racer) {
  await ddb.send(
    new PutCommand({
      TableName: RACERS_TABLE,
      Item: {
        racerId: r.id,
        teamId: r.teamId,
        name: r.name,
        gender: r.gender,
        class: r.class,
      },
    })
  );
}

async function putRace(rc: Race) {
  await ddb.send(
    new PutCommand({
      TableName: RACES_TABLE,
      Item: {
        raceId: rc.id,
        name: rc.name,
        location: rc.location,
        date: rc.date,
        type: rc.type,
      },
    })
  );
}

// ======== MAIN ========
(async function main() {
  console.log(`Seeding to region=${REGION}`);
  console.log(`Tables: TEAMS=${TEAMS_TABLE}, RACERS=${RACERS_TABLE}, RACES=${RACES_TABLE}`);

  console.log(`\n== Writing Teams (${finalTeams.length}) ==`);
  for (const t of finalTeams) {
    await putTeam(t);
    console.log(`  ✓ ${t.id} — ${t.name}`);
  }

  console.log(`\n== Writing Racers (${dedupedRacers.length}) ==`);
  for (const r of dedupedRacers) {
    await putRacer(r);
    console.log(`  ✓ ${r.id} — ${r.name} (${r.gender}, ${r.class}) [${r.teamId}]`);
  }

  console.log(`\n== Writing Races (${races.length}) ==`);
  for (const rc of races) {
    await putRace(rc);
    console.log(`  ✓ ${rc.id} — ${rc.name} (${rc.type}) on ${rc.date}`);
  }

  console.log("\nAll done ✅");
})().catch((err) => {
  console.error("Seed failed:", err);
  process.exit(1);
});
