#!/usr/bin/env node

console.log('Command line arguments:', process.argv);

const AUTH_API_URL = process.argv[2] || 'http://localhost:3000/graphql';
const JOBS_API_URL = process.argv[3] || 'http://localhost:3001/graphql';
const BATCH_SIZE = parseInt(process.argv[4], 10) || 3;

const LOGIN_MUTATION = `
  mutation Login($loginInput: LoginInput!) {
    login(loginInput: $loginInput) {
      id
    }
  }
`;

const EXECUTE_JOB_MUTATION = `
  mutation ExecuteJob($executeJobInput: ExecuteJobInput!) {
    executeJob(executeJobInput: $executeJobInput) {
      name
    }
  }
`;

async function login(email, password) {
  console.log(`[Auth] Logging in as ${email}`);
  const response = await fetch(AUTH_API_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      query: LOGIN_MUTATION,
      variables: { loginInput: { email, password } },
    }),
  });

  const data = await response.json();
  const cookies = response.headers.get('set-cookie');
  if (!data?.data?.login?.id) {
    throw new Error(`❌ Login failed: ${JSON.stringify(data.errors)}`);
  }
  console.log(`[Auth] ✅ Login successful`);
  return cookies;
}

async function executeJobWithInput(executeJobInput, cookies) {
  const response = await fetch(JOBS_API_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Cookie: cookies },
    body: JSON.stringify({
      query: EXECUTE_JOB_MUTATION,
      variables: { executeJobInput },
    }),
  });

  const data = await response.json();
  if (data.errors) console.error('❌ GraphQL Error:', data.errors);
  return data;
}

function chunkArray(arr, size) {
  const batches = [];
  for (let i = 0; i < arr.length; i += size) {
    batches.push(arr.slice(i, i + size));
  }
  return batches;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

(async () => {
  try {
    const cookies = await login('test@gmail.com', 'Tester@123');

    const campaignIds = [
      '8e0cda49-80ae-4861-852d-70eb1b1c52ff',
      '8325a753-3dcb-4366-8284-457f31ab048a',
      'c544adf1-d047-45ed-95fc-5c8bad9fe7a8',
      '9fd1a39f-ef34-4c25-875b-1e0d4809f0ac',
      'fddff0f9-9489-4777-b180-65e1dc1bcf85',
      'f1a6d151-34f9-4ef5-9dfa-afdad0dbb272',
      'c8afefb2-ed74-43be-a722-f0b0b78dd939',
      '599afb9b-54cf-4ff6-98b3-c6dbfbe0c140',
      'bfcc63e3-387a-4d74-be8d-9870796208ee',
      '9dadc006-dc19-4fac-bea1-afd850666ec6',
      '494796eb-693a-4dbd-823b-24af75567158',
      'b995140d-6ca3-4723-ae32-b71ffcb12f11',
      '0beb9e77-6c1c-48bd-8686-c407b59946bf',
      '15ffac8e-191d-44a8-808c-f83b8ac3b61c',
    ];

    const batches = chunkArray(campaignIds, BATCH_SIZE);
    console.log(`📦 Found ${batches.length} batches (${BATCH_SIZE} per batch)`);

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      console.log(`\n🚀 Sending batch ${i + 1}/${batches.length}:`, batch);

      const executeJobInput = {
        name: 'ClickhouseSchedule',
        data: batch.map((id) => ({ campaignId: id })),
      };

      const res = await executeJobWithInput(executeJobInput, cookies);
      console.log(`[✅] Batch ${i + 1} result:`, res?.data || res?.errors);

      await sleep(1000);
    }

    console.log('\n🎉 All batches executed successfully.');
  } catch (err) {
    console.error('❌ Error in execution:', err);
  }
})();
