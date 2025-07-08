using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace FlowGui.App
{
    public class AutoSetup
    {
        public async Task SetupAsync()
        {
            try
            {
                string repo = "valek77/PyFlowETL";
                string versionFile = Path.Combine("etl_framework", "VERSION.txt");
                string venvPythonExe = Path.Combine("etl_venv", "Scripts", "python.exe");

                LogManager.Append("🔍 Controllo aggiornamenti framework...");
                await DownloadFrameworkIfNeeded(repo, versionFile);

                LogManager.Append("⚙️ Verifica venv...");
                CreateVenvIfMissing(venvPythonExe);

                LogManager.Append("📦 Installazione dipendenze...");
                await InstallFrameworkDependencies(venvPythonExe);

                LogManager.Append("✅ Setup completato.");
            }
            catch (Exception ex)
            {
                LogManager.Append($"[AutoSetup ERROR] {ex.Message}", isError: true);
            }
        }

        private async Task DownloadFrameworkIfNeeded(string repo, string versionFile)
        {
            using var client = new HttpClient();
            client.DefaultRequestHeaders.UserAgent.ParseAdd("PyFlowETL-Manager");

            var json = await client.GetStringAsync($"https://api.github.com/repos/{repo}/releases/latest");
            var obj = JObject.Parse(json);
            string tag = obj["tag_name"]!.ToString();
            string currentVersion = File.Exists(versionFile) ? File.ReadAllText(versionFile).Trim() : "";

            if (currentVersion == tag)
            {
                LogManager.Append($"🟢 Framework già aggiornato ({tag})");
                return;
            }

            LogManager.Append($"⬇️ Scarico PyFlowETL {tag} da GitHub...");

            string zipUrl = $"https://github.com/{repo}/archive/refs/tags/{tag}.zip";
            string zipPath = Path.Combine(Path.GetTempPath(), $"pyflowetl_{tag}.zip");

            var bytes = await client.GetByteArrayAsync(zipUrl);
            await File.WriteAllBytesAsync(zipPath, bytes);

            if (Directory.Exists("etl_framework"))
            {
                LogManager.Append("🧹 Rimuovo versione precedente...");
                Directory.Delete("etl_framework", true);
            }

            LogManager.Append("📦 Estrazione nuova versione...");
            ZipFile.ExtractToDirectory(zipPath, Directory.GetCurrentDirectory());
            string extracted = Path.Combine(Directory.GetCurrentDirectory(), $"PyFlowETL-{tag.TrimStart('v')}");
            Directory.Move(extracted, "etl_framework");

            File.WriteAllText(versionFile, tag);
            LogManager.Append($"✅ Framework aggiornato a {tag}");
        }

        private void CreateVenvIfMissing(string pythonExe)
        {
            if (File.Exists(pythonExe))
            {
                LogManager.Append("🔁 Venv già presente.");
                return;
            }

            LogManager.Append("🛠️ Creo ambiente virtuale (venv)...");

            string systemPython = "python";
            var psi = new ProcessStartInfo
            {
                FileName = systemPython,
                Arguments = "-m venv etl_venv",
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            };

            var p = Process.Start(psi);
            p.OutputDataReceived += (s, e) => LogManager.Append(e.Data);
            p.ErrorDataReceived += (s, e) => LogManager.Append(e.Data, isError: true);
            p.BeginOutputReadLine();
            p.BeginErrorReadLine();
            p.WaitForExit();
        }

        private async Task InstallFrameworkDependencies(string pythonExe)
        {
            string requirements = Path.Combine("etl_framework", "requirements.txt");

            if (!File.Exists(requirements))
            {
                LogManager.Append("⚠️ requirements.txt non trovato. Salto installazione dipendenze.", isError: true);
                return;
            }

            LogManager.Append("📦 pip install -r requirements.txt");

            var pip = new ProcessStartInfo
            {
                FileName = pythonExe,
                Arguments = $"-m pip install -r \"{Path.GetFullPath(requirements)}\"",
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };

            var p = new Process { StartInfo = pip };
            p.OutputDataReceived += (s, e) => LogManager.Append(e.Data);
            p.ErrorDataReceived += (s, e) => LogManager.Append(e.Data, isError: true);
            p.Start();
            p.BeginOutputReadLine();
            p.BeginErrorReadLine();
            await p.WaitForExitAsync();

            LogManager.Append("🔗 pip install -e ./etl_framework");

            var install = new ProcessStartInfo
            {
                FileName = pythonExe,
                Arguments = "-m pip install -e ./etl_framework",
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
                WorkingDirectory = AppDomain.CurrentDomain.BaseDirectory
            };

            var proc = new Process { StartInfo = install };
            proc.OutputDataReceived += (s, e) => LogManager.Append(e.Data);
            proc.ErrorDataReceived += (s, e) => LogManager.Append(e.Data, isError: true);
            proc.Start();
            proc.BeginOutputReadLine();
            proc.BeginErrorReadLine();
            await proc.WaitForExitAsync();
        }

    }
}
