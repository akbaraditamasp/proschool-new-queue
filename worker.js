const mysql = require("mysql2");
const Queue = require("bee-queue");
const fs = require("fs");

const isDev = (process.env.NODE_ENV || "development") === "development";

if (isDev) {
  require("dotenv").config();
}

const queue = new Queue("saving_worksheet");

console.log("running...");

const checkRun = (id, soal_id) =>
  new Promise((resolve) => {
    connection.query(
      "SELECT tessoal_tesuser_id FROM cbt_tes_soal WHERE tessoal_tesuser_id=? AND tessoal_soal_id=?",
      [id, soal_id],
      (err, res) => {
        if (err) {
          resolve(null);
          return;
        }

        resolve(res.length ? res[0] : null);
      }
    );
  });

const checkChildrenRun = (soaljawaban_jawaban_id) =>
  new Promise((resolve) => {
    connection.query(
      "SELECT soaljawaban_tessoal_id FROM cbt_tes_soal_jawaban WHERE soaljawaban_jawaban_id=?",
      [soaljawaban_jawaban_id],
      (err, res) => {
        if (err) {
          resolve(null);
          return;
        }

        resolve(res.length ? res[0] : null);
      }
    );
  });

const connection = mysql.createConnection({
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  database: process.env.MYSQL_DB,
  password: process.env.MYSQL_PASS,
});

const processing = (task, children = []) =>
  new Promise(async (resolve) => {
    try {
      let newTask = task;
      const match =
        /INSERT INTO cbt_tes_soal (.*) VALUES \(\'([0-9]+)\', \'([0-9]+)\', \'(.*)\', \'(.*)\'/gm.exec(
          task
        );

      const tessoal_tesuser_id = match ? match[2] : null;
      const tessoal_soal_id = match ? match[3] : null;
      const tessoal_nilai = match ? match[4] : null;
      const tessoal_creation_time = match ? match[5] : null;

      const checkTes = match
        ? await checkRun(tessoal_tesuser_id, tessoal_soal_id)
        : null;

      if (checkTes && checkTes.tessoal_tesuser_id) {
        newTask = `UPDATE cbt_tes_soal SET tessoal_nilai='${tessoal_nilai}' WHERE tessoal_tesuser_id='${tessoal_tesuser_id}' AND tessoal_soal_id='${tessoal_soal_id}'`;
      }

      connection.query(newTask, async (err, result) => {
        if (err) {
          console.log(err);
          resolve(false);
          return;
        }

        let failed = false;
        for (const query of children) {
          let id = null;
          if (!result.insertId && !(checkTes && checkTes.tessoal_tesuser_id)) {
            failed = true;
            break;
          } else {
            id = result.insertId || checkTes.tessoal_tesuser_id;
          }

          let newTask = query.task.replace("%id%", id);

          if (checkTes && checkTes.tessoal_tesuser_id) {
            const match =
              /INSERT INTO cbt_tes_soal_jawaban (.*) VALUES \(\'%id%\', \'([0-9]+)\', \'(.*)\', \'(.*)\'/gm.exec(
                query.task
              );

            const soaljawaban_jawaban_id = match ? match[2] : null;
            const soaljawaban_selected = match ? match[3] : null;
            const soaljawaban_order = match ? match[4] : null;

            const checkChildrenTes = match
              ? await checkChildrenRun(soaljawaban_jawaban_id)
              : null;

            if (checkChildrenTes && checkChildrenTes.soaljawaban_tessoal_id) {
              newTask = `UPDATE cbt_tes_soal_jawaban SET soaljawaban_selected='${soaljawaban_selected}' WHERE soaljawaban_jawaban_id='${soaljawaban_jawaban_id}'`;
            }
          }

          if (!(await processing(newTask, query.children || []))) {
            failed = true;
            break;
          }
        }

        resolve(!failed);
      });
    } catch (e) {
      console.log(e);
      resolve(false);
    }
  });

queue.process(async (job, done) => {
  if (await processing(job.data.task, job.data.children || [])) {
    return done(null, null);
  } else {
    return done(new Error("Error"), null);
  }
});

queue.on("succeeded", (job) => {
  if (fs.existsSync(`./failed_jobs2/${job.data.key}.json`)) {
    fs.unlinkSync(`./failed_jobs2/${job.data.key}.json`);
  }

  fs.writeFileSync(
    `./succeed_jobs/${job.data.key}.json`,
    JSON.stringify(job.data)
  );

  console.log(job.data.key + "-- BERHASIL --");
});

queue.on("failed", (job) => {
  fs.writeFileSync(
    `./failed_jobs2/${job.data.key}.json`,
    JSON.stringify(job.data)
  );

  console.log(job.data.key + "-- GAGAL --");
});
