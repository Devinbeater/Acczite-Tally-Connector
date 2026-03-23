// Acczite/Services/TallyIntegrationService.cs
using System;
using System.Diagnostics;  // For Process
using System.IO;           // For Path
using System.Threading.Tasks;
using System.Windows;  // For MessageBox

namespace Acczite20.Services
{
    public class TallyIntegrationService
    {
        public async Task<bool> SyncTallyData()
        {
            // 1.  Get the path to the python script.
            string scriptPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "sync_script.py"); // or the location
            if (!File.Exists(scriptPath))
            {
                MessageBox.Show("Error: The Tally sync script (sync_script.py) was not found.");
                return false;
            }
            // 2.  Get the path to the python executable
            string pythonPath = "python"; // or full path if necessary -  "C:\\Python39\\python.exe"
            // 3. Create a ProcessStartInfo
            ProcessStartInfo psi = new ProcessStartInfo
            {
                FileName = pythonPath,
                Arguments = $"\"{scriptPath}\"", // Pass the script path
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,  //  Important for redirection
                CreateNoWindow = true    //  Hide the console window.
            };

            // 4.  Start the process
            Process process = new Process { StartInfo = psi };
            process.Start();

            // 5. Read the output and error streams
            string output = await process.StandardOutput.ReadToEndAsync();
            string error = await process.StandardError.ReadToEndAsync();

            process.WaitForExit();

            // 6.  Process the Results.
            if (process.ExitCode == 0)
            {
                MessageBox.Show($"Tally sync completed successfully.\nOutput: {output}");
                return true;
            }
            else
            {
                MessageBox.Show($"Tally sync failed.\nError: {error}\nOutput: {output}");
                return false;
            }
        }
    }
}