const fs = require("fs");
const path = require("path");
const Queue = require("bee-queue");

require("dotenv").config();

const queue = new Queue("saving_worksheet");

const folderPath = "./failed_jobs";

const getFiles = () =>
  new Promise((resolve) => {
    fs.readdir(folderPath, (err, files) => {
      if (err) {
        console.error("Error membaca folder:", err);
        return;
      }

      const jobs = files
        .filter((file) => file.endsWith(".json"))
        .map((file) => path.join(folderPath, file));

      resolve(jobs);
    });
  });

const getJob = (job) =>
  new Promise((resolve) => {
    fs.readFile(job, "utf8", (err, data) => {
      if (err) {
        console.error(`Error membaca file ${job}:`, err);
        resolve(null);
      } else {
        let jsonData = {};
        try {
          jsonData = JSON.parse(data);
        } catch (jsonErr) {
          console.error(`File ${job} tidak valid JSON:`, jsonErr.message);
          resolve(null);
          return;
        }

        resolve(jsonData);
      }
    });
  });

(async () => {
  for (const job of await getFiles()) {
    const jobData = await getJob(job);

    if (jobData) {
      const init = queue.createJob(jobData);
      console.log("run " + jobData.key);

      await init.timeout(3000).retries(0).save();
    } else {
      console.log(jobData);
    }
  }
})();
