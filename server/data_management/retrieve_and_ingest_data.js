const fs = require('fs');
const path = require('path');
const express = require('express');
const router = express.Router();
const client = require('../elasticsearch/client');
require('log-timestamp');

// Percorso locale del file CSV
const filePath = path.join('/Users/davide/Desktop/archive/Twitch_game_data.csv');

router.get('/twitch-games', async function (req, res) {
  console.log('Loading Application...');
  res.json('Running Application...');

  const indexData = async () => {
    try {
      console.log('Reading data from local CSV file...');
      
      // Leggi il file CSV localmente
      const csvData = fs.readFileSync(filePath, 'utf8');
      
      const rows = csvData.split('\n').slice(1); // Salta l'intestazione

      console.log('Indexing data...');
      rows.forEach(async (row) => {
        const fields = row.split(',');
        if (fields.length >= 12) {
          const gameObject = {
            Game: fields[0].trim(),
            Avg_viewers: parseInt(fields[1], 10),
            Streamers: parseInt(fields[2], 10),
            Avg_viewer_ratio: parseFloat(fields[3]),
            Avg_channels: parseInt(fields[4], 10),
            Rank: parseInt(fields[5], 10),
            Peak_viewers: parseInt(fields[6], 10),
            Month: parseInt(fields[7], 10),
            year: parseInt(fields[8], 10),
            Hours_streamed: parseInt(fields[9], 10),
            Hours_watched: parseInt(fields[10], 10),
            Peak_channels: parseInt(fields[11], 10)
          };

          console.log(JSON.stringify(gameObject, null, 2));

          // Indicizza il documento in Elasticsearch
          await client.index({
            index: 'twitch_top_game',
            body: gameObject,
            pipeline: 'Twitch_data_pipeline',
          });
        }
      });

      console.log('Data has been indexed successfully!');
    } catch (err) {
      console.error('Error during indexing:', err);
    }
  };

  indexData();
});

module.exports = router;
