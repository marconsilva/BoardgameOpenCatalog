using Microsoft.Azure.Cosmos.Table;
using System;
using System.Collections.Generic;
using System.Text;

namespace DICE.Data.Ingest.Bgg.BoardgameDataImport
{
    public class BoardgameReferenceTableListEntity : TableEntity
    {
        public int Id { get; set; }
        public string BGGPageUrl { get; set; }
        public int BGGRankingNumber { get; set; }
        public string Title { get; set; }
        public float GeekRating { get; set; }
        public float AverageRating { get; set; }
        public long NumberOfVoters { get; set; }
        public int Year { get; set; }
    }
}
