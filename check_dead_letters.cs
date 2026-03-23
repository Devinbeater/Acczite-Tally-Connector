using System;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Acczite20.Data;
using Acczite20.Models;

var services = new ServiceCollection();
services.AddDbContext<AppDbContext>(options =>
    options.UseSqlServer("Server=localhost;Database=Acczite20;Trusted_Connection=True;TrustServerCertificate=True;"));

var serviceProvider = services.BuildServiceProvider();

using (var scope = serviceProvider.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    var deadLetters = db.DeadLetters.OrderByDescending(d => d.DetectedAt).Take(10).ToList();
    
    Console.WriteLine($"Total Dead-Letters found: {db.DeadLetters.Count()}");
    foreach (var dl in deadLetters)
    {
        Console.WriteLine($"ID: {dl.TallyMasterId} | Reason: {dl.ErrorReason} | Time: {dl.DetectedAt}");
    }
}
