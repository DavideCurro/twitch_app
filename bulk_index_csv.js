const fs = require("fs");
const path = require("path");
const { Client } = require("@elastic/elasticsearch");

// Configura Elasticsearch
const client = new Client({
  cloud: {
    id: "cloudid",
  },
  auth: {
    apiKey: "apikey", // Sostituisci con la tua API Key
  },
});

const BULK_SIZE = 1000; 
const TOTAL_DOCS = 22000; 
const INDEX_NAME = "twitch_top_game";
const PIPELINE_NAME = "Twitch_data_pipeline";
const CSV_PATH = "Percorso_csv"; 

async function generateAndIndexData() {
  try {
    console.log("Reading CSV file...");
    const csvData = fs.readFileSync(CSV_PATH, "utf8");

    // Parso il CSV: salto l'intestazione
    const rows = csvData.split("\n").slice(1).filter((row) => row.trim() !== "");
    const bulkBody = [];
    let documentCount = 0;

    console.log("Preparing data for indexing...");

    while (documentCount < TOTAL_DOCS) {
      for (let i = 0; i < rows.length && documentCount < TOTAL_DOCS; i++) {
        const fields = rows[i].split(",");

        
const gameObject = {
  Rank: parseInt(fields[0], 10) || 0,                      
  Game: fields[1]?.trim() || "Unknown Game",               
  Month: parseInt(fields[2], 10) || 0,                    
  Year: parseInt(fields[3], 10) || 0,                     
  Hours_watched: isNaN(parseInt(fields[4], 10)) ? 0 : Number(fields[4]), 
  Hours_streamed: isNaN(parseInt(fields[5], 10)) ? 0 : Number(fields[5]), 
  Peak_viewers: parseInt(fields[6], 10) || 0,             
  Peak_channels: parseInt(fields[7], 10) || 0,            
  Streamers: parseInt(fields[8], 10) || 0,                
  Avg_viewers: parseInt(fields[9], 10) || 0,              
  Avg_channels: parseInt(fields[10], 10) || 0,            
  Avg_viewer_ratio: fields[11] ? parseFloat(fields[11]) : 0.0, 
};


        // Aggiunge l'operazione bulk per il documento
        bulkBody.push({ index: { _index: INDEX_NAME } });
        bulkBody.push(gameObject);

        documentCount++;
      }

      // Invia il batch di documenti a Elasticsearch con la pipeline
      console.log(`Sending batch of ${bulkBody.length / 2} documents...`);
      const response = await client.bulk({
        body: bulkBody,
        pipeline: PIPELINE_NAME,
      });

      // Controlla errori nella Bulk API
      if (response.errors) {
        console.error("Bulk indexing encountered errors!");
        const erroredItems = response.items.filter(
          (item) => item.index && item.index.error
        );
        console.log(
          "Errored items:",
          erroredItems.map((item) => item.index.error)
        );
      } else {
        console.log(
          `Batch successfully indexed: ${bulkBody.length / 2} documents.`
        );
      }

      bulkBody.length = 0; 
    }

    console.log(`Successfully indexed ${documentCount} documents!`);
  } catch (error) {
    console.error("Error during indexing:", error);
  }
}

generateAndIndexData();

