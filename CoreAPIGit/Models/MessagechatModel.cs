namespace CoreAPIGit.Models
{
    public class MessagechatModel
    {									
        public Guid ChatID  { get; set; }
        public Guid SessionID { get; set; }
        public long FromUser { get; set; }
        public long ToUser { get; set; }
        public string MessageChat { get; set; }
        public string MessageChatUrl { get; set; }
        public DateTime CreatedDate { get; set; }
        public int IsWatched { get; set; }
        public DateTime DateWatched { get; set; }
        public int SequenceNumber { get; set; }
    }
}
