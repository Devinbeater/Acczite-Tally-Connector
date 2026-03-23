using System;
using System.IO;
using System.Threading.Tasks;
using Acczite20.Models.Analytics;

namespace Acczite20.Services.Analytics
{
    public interface IReportingService
    {
        Task<string> ExportWeeklyReportAsync(BusinessPulseStats stats);
    }

    public class ReportingService : IReportingService
    {
        public async Task<string> ExportWeeklyReportAsync(BusinessPulseStats stats)
        {
            // In a real app, use QuestPDF or EPPlus
            var path = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), $"WeeklyReport_{DateTime.Now:yyyyMMdd}.txt");
            
            using var sw = new StreamWriter(path);
            await sw.WriteLineAsync("ACCZITE 2.0 - WEEKLY BUSINESS PULSE REPORT");
            await sw.WriteLineAsync("===========================================");
            await sw.WriteLineAsync($"Generated: {DateTime.Now}");
            await sw.WriteLineAsync("");
            await sw.WriteLineAsync($"Today's Sales: ₹ {stats.SalesToday:N2}");
            await sw.WriteLineAsync($"Total Receivables: ₹ {stats.TotalReceivables:N2}");
            await sw.WriteLineAsync($"Total Payables: ₹ {stats.TotalPayables:N2}");
            await sw.WriteLineAsync("");
            await sw.WriteLineAsync("ALERTS & INSIGHTS:");
            foreach(var alert in stats.Alerts)
            {
                await sw.WriteLineAsync($"- [{alert.Severity}] {alert.Category}: {alert.Message}");
            }
            
            return path;
        }
    }
}
