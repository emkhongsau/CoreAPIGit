using CoreAPIGit.Models;
using Microsoft.EntityFrameworkCore;
using System.Data;

namespace CoreAPIGit
{
    public class DatabaseContext : DbContext
    {
        public DatabaseContext(DbContextOptions options): base(options) { }

        public DbSet<MessagechatModel> MessagechatModels { get; set; }
    }
}
