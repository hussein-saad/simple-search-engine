const fs = require("fs");

const url_to_file_map = JSON.parse(fs.readFileSync("url_to_file_map.json"));

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
        const freq = url[1];
        const total = url[2];
        if (!invertedIndexDict[word]) {
          invertedIndexDict[word] = [];
        }
        invertedIndexDict[word].push(`${name}.txt ${freq} ${total}`);
      });
    }
  });

  results = search(invertedIndexDict);
}

function search(invertedIndex, query) {
  query = query.split(" ");

  const pages = new Set();

  query.forEach((word) => {
    word = word.toLowerCase();
    if (invertedIndex[word]) {
      invertedIndex[word].forEach((page) => {
        pages.add(page);
      });
    }
  });

  const totalDocuments = pages.size;

  let tfidfScores = {};

  query.forEach((word) => {
    word = escapeRegExp(word);
    if (invertedIndex[word]) {
      invertedIndex[word].forEach((docFreqTotal) => {
        let [page, freq, total] = docFreqTotal.split(" ");
        const tf = freq / total;
        const idf = Math.log(totalDocuments / invertedIndex[word].length);
        const tfidf = tf * idf;

        if (url_to_file_map[page]) {
          page = url_to_file_map[page];

          if (!tfidfScores[page]) {
            tfidfScores[page] = 0;
          }

          tfidfScores[page] += tfidf;
        }
      });
    }
  });

  let tfidfScoresArray = Object.entries(tfidfScores);

  tfidfScoresArray.sort((a, b) => b[1] - a[1]);
}

main();
