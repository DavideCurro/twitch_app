const express = require('express');
const cors = require('cors');
const client = require('./elasticsearch/client'); // Importa il client Elasticsearch configurato

const app = express();

// Abilita CORS per le richieste da React
app.use(cors());

// Endpoint per verificare la connessione
app.get('/health', async (req, res) => {
  try {
    const response = await client.ping();
    console.log('Elasticsearch is connected:', response);
    res.status(200).json({ message: 'Connected to Elasticsearch!' });
  } catch (error) {
    console.error('Error connecting to Elasticsearch:', error);
    res.status(500).json({ error: 'Elasticsearch is not connected.' });
  }
});

// Endpoint per ottenere i risultati con tutti i campi
app.get('/results', async (req, res) => {
  const { game } = req.query;

  console.log('Received query for game:', game); 

  try {
    const query = game
      ? { match: { Game: game } } // Cerca per nome del gioco
      : { match_all: {} }; // Recupera tutti i risultati

    console.log('Elasticsearch query:', JSON.stringify(query, null, 2)); 

    const response = await client.search({
      index: 'twitch_top_game',
      size: game ? 1 : 1000, // Limita il numero di risultati
      query,
      _source: [
        "Game",
        "Rank",
        "Hours_watched",
        "Hours_streamed",
        "Peak_viewers",
        "Peak_channels",
        "Avg_viewers",
        "Avg_channels",
        "Avg_viewer_ratio",
        "Streamers",
        "Month",
        "year"
      ], 
    });

    console.log('Elasticsearch response:', response.hits.hits); 

    // Estrai solo il contenuto dei documenti
    const results = response.hits.hits.map((hit) => hit._source);
    res.json(results);
  } catch (error) {
    console.error('Error querying Elasticsearch:', error.meta ? error.meta.body : error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});


// Porta del server
const PORT = 3002;
app.listen(PORT, () => console.log(`Server running on http://localhost:${PORT}`));






