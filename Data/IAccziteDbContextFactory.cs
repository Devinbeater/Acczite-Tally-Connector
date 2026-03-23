using Microsoft.EntityFrameworkCore;

namespace Acczite20.Data;

public interface IAccziteDbContextFactory
{
    AccziteDbContext CreateDbContext();
}