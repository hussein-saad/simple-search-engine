const fs = require("fs");

function calculatePageRank(linksGraph, dampingFactor = 0.85, iterations = 50) {
  const pages = Object.keys(linksGraph);
  const pageRank = {};
  const numPages = pages.length;

  const linksGraphSet = {};
  for (let page in linksGraph) {
    linksGraphSet[page] = new Set(linksGraph[page]);
  }

  pages.forEach((page) => {
    pageRank[page] = 1 / numPages;
  });

  for (let i = 0; i < iterations; i++) {
    const newPageRank = {};

    pages.forEach((page) => {
      let inboundSum = 0;

      pages.forEach((innerPage) => {
        if (linksGraphSet[innerPage].has(page)) {
          inboundSum += pageRank[innerPage] / linksGraph[innerPage].length;
        }
      });

      newPageRank[page] =
        (1 - dampingFactor) / numPages + dampingFactor * inboundSum;
    });

    Object.assign(pageRank, newPageRank);
  }

  const sortedPages = Object.keys(pageRank).sort(
    (a, b) => pageRank[b] - pageRank[a]
  );

  console.log("PageRank results:");

  let data = {};
  sortedPages.forEach((page) => {
    data[page] = pageRank[page].toFixed(10);
  });

  fs.writeFile("pageRankResults.json", JSON.stringify(data, null, 2), (err) => {
    if (err) {
      console.error("Error writing file:", err);
    } else {
      console.log("PageRank results written to pageRankResults.json");
    }
  });
  
}

module.exports = calculatePageRank;
