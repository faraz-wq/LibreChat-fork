const path = require('path');
const crypto = require('crypto');
const { promises: fs } = require('fs');
const { logger } = require('@librechat/data-schemas');

const LOCAL_SKILLS_DIR = process.env.LOCAL_SKILLS_DIR ?? path.join(process.cwd(), 'skills');

/**
 * Local-mode replacement for batchUploadCodeEnvFiles.
 *
 * Instead of POSTing to the remote Code API, writes each stream directly
 * to LOCAL_SKILLS_DIR/{filename} on the host filesystem so the
 * LocalExecutionEngine can read them from its working directory.
 *
 * Returns the same shape as the remote implementation so all callers
 * (primeSkillFiles, primeInvokedSkills) work without changes.
 */
async function batchUploadCodeEnvFilesLocal({ files, kind, id, version }) {
  const storageSessionId = `local-${kind}-${id}${version != null ? `-v${version}` : ''}`;

  const uploaded = await Promise.allSettled(
    files.map(async ({ stream, filename }) => {
      const dest = path.join(LOCAL_SKILLS_DIR, filename);
      await fs.mkdir(path.dirname(dest), { recursive: true });

      const chunks = [];
      await new Promise((resolve, reject) => {
        stream.on('data', (chunk) => chunks.push(chunk));
        stream.on('end', resolve);
        stream.on('error', reject);
      });

      await fs.writeFile(dest, Buffer.concat(chunks));
      return { fileId: crypto.randomUUID(), filename };
    }),
  );

  const successFiles = [];
  let failed = 0;
  for (const result of uploaded) {
    if (result.status === 'fulfilled') {
      successFiles.push(result.value);
    } else {
      failed++;
      logger.warn('[batchUploadCodeEnvFilesLocal] File write failed:', result.reason);
    }
  }

  if (failed > 0) {
    logger.warn(`[batchUploadCodeEnvFilesLocal] ${failed} file(s) failed to write to local disk`);
  }

  return { storage_session_id: storageSessionId, files: successFiles };
}

module.exports = { batchUploadCodeEnvFilesLocal };
