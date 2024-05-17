const fs = require("fs");

const url_to_file_map = JSON.parse(fs.readFileSync("url_to_file_map.json"));
const pageRankResults = JSON.parse(fs.readFileSync("pageRankResults.json"));

let results = [];

async function main() {
  const invertedIndex = await fs.promises.readFile(
    "./invertedIndex/output/part-r-00000",
    "utf8"
  );

  const lines = invertedIndex.split("\n");

  const invertedIndexDict = {};

  lines.forEach((line) => {
    let [word, urls] = line.split("\t");

    if (urls) {
      urls = urls.split(";");
      urls.forEach((url) => {
        url = url.split(":");
        const name = url[0];
        if (!invertedIndexDict[word]) {
          invertedIndexDict[word] = [];
        }
        invertedIndexDict[word].push(`${name}.txt`);
      });
    }
  });

  results = search(invertedIndexDict);
  console.log(results);
}

function search(invertedIndex, query) {
  query = query.split(" ");

  const pages = new Set();

  query.forEach((word) => {
    word = word.toLowerCase();
    if (invertedIndex[word]) {
      invertedIndex[word].forEach((page) => pages.add(page));
    }
  });

  const urls = [];

  pages.forEach((page) => {
    if (url_to_file_map[page]) urls.push(url_to_file_map[page]);
  });

  urls.sort((a, b) => {
    const scoreA = pageRankResults[a] || 0;
    const scoreB = pageRankResults[b] || 0;
    return scoreB - scoreA;
  });

  return urls;
}

main();
