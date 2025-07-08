using FlowGui.App;
using System.IO;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;
using System.Net.Http;
using System.IO.Compression;
using Newtonsoft.Json.Linq;

using System.Diagnostics;
using System.Text;


namespace FlowGui
{
    public partial class MainForm : Form
    {
        public MainForm()
        {
            InitializeComponent();
        }

        private async void MainForm_Load(object sender, EventArgs e)
        {
            LogManager.SetOutput(txtLog);
            LoadJobs();
            await new AutoSetup().SetupAsync();
        }





        private void LoadJobs()
        {
            cmbJobs.Items.Clear();

            cmbJobs.Items.Add(new JobDefinition
            {
                Name = "-- Seleziona un Job --",         // oppure "-- Seleziona un Job --"
                Description = "",
                Path = ""
            });

            string jobsRoot = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "jobs");
            if (!Directory.Exists(jobsRoot))
            {
                MessageBox.Show("Cartella jobs non trovata");
                return;
            }

            var deserializer = new DeserializerBuilder()
                .WithNamingConvention(UnderscoredNamingConvention.Instance)
                .Build();

            foreach (string jobDir in Directory.GetDirectories(jobsRoot))
            {
                string yamlPath = Path.Combine(jobDir, "job.yaml");
                if (!File.Exists(yamlPath))
                    continue;

                try
                {
                    var yaml = File.ReadAllText(yamlPath);
                    var raw = deserializer.Deserialize<Dictionary<string, object>>(yaml);

                    string name = raw.ContainsKey("name") ? raw["name"].ToString() : Path.GetFileName(jobDir);
                    string description = raw.ContainsKey("description") ? raw["description"].ToString() : "";

                    var job = new JobDefinition
                    {
                        Name = name,
                        Description = description,
                        Path = jobDir
                    };

                    cmbJobs.Items.Add(job);
                }
                catch (Exception ex)
                {
                    MessageBox.Show($"Errore nel parsing di {yamlPath}: {ex.Message}");
                }
            }

            if (cmbJobs.Items.Count > 0)
                cmbJobs.SelectedIndex = 0;
        }


       

        private void GenerateParameterControls(string jobFolder)
        {
            pnlParameters.Controls.Clear();

            string yamlPath = Path.Combine(jobFolder, "job.yaml");
            if (!File.Exists(yamlPath))
            {
                MessageBox.Show("job.yaml non trovato.");
                return;
            }

            var deserializer = new DeserializerBuilder()
                .WithNamingConvention(UnderscoredNamingConvention.Instance)
                .Build();

            var yamlText = File.ReadAllText(yamlPath);
            var yamlData = deserializer.Deserialize<Dictionary<string, object>>(yamlText);

            if (!yamlData.TryGetValue("parameters", out var paramListRaw) || paramListRaw is not List<object> paramList)
            {
                return;
            }

            var description = yamlData.ContainsKey("description") ? yamlData["description"].ToString() : "Nessuna descrizione disponibile.";

            txtDescription.Text = description;

            foreach (var item in paramList)
            {
                var dict = item as Dictionary<object, object>;
                var param = new JobParameter
                {
                    Name = dict["name"].ToString(),
                    Type = dict["type"].ToString(),
                    Label = dict.ContainsKey("label") ? dict["label"].ToString() : dict["name"].ToString(),
                    Default = dict.ContainsKey("default") ? dict["default"].ToString() : ""
                };

                Label lbl = new Label { Text = param.Label, AutoSize = true };

                Control inputControl;
                switch (param.Type)
                {
                    case "string":
                    case "integer":
                        inputControl = new TextBox { Text = param.Default ?? "", Width = 300 };
                        break;

                    case "boolean":
                        inputControl = new CheckBox { Checked = param.Default?.ToLower() == "true" };
                        break;

                    case "file":
                    case "folder":
                        var panel = new FlowLayoutPanel { AutoSize = true, FlowDirection = FlowDirection.LeftToRight };
                        var txt = new TextBox { Text = param.Default ?? "", Width = 550 };
                        var btn = new Button { Text = "...", Width = 30, Tag = (param.Type, txt) };
                        btn.Click += (s, e) =>
                        {
                            if (param.Type == "file")
                            {
                                var ofd = new OpenFileDialog();
                                if (ofd.ShowDialog() == DialogResult.OK)
                                    txt.Text = ofd.FileName;
                            }
                            else
                            {
                                var fbd = new FolderBrowserDialog();
                                if (fbd.ShowDialog() == DialogResult.OK)
                                    txt.Text = fbd.SelectedPath;
                            }
                        };
                        panel.Controls.Add(txt);
                        panel.Controls.Add(btn);
                        panel.Tag = param.Name;
                        pnlParameters.Controls.Add(lbl);
                        pnlParameters.Controls.Add(panel);
                        continue;

                    default:
                        inputControl = new TextBox { Text = param.Default ?? "", Width = 300 };
                        break;
                }

                inputControl.Tag = param.Name;
                pnlParameters.Controls.Add(lbl);
                pnlParameters.Controls.Add(inputControl);
            }
        }

        private void cmbJobs_SelectedValueChanged(object sender, EventArgs e)
        {

        }

        private void cmbJobs_SelectedIndexChanged(object sender, EventArgs e)
        {
            var job = cmbJobs.SelectedItem as JobDefinition;
            if (!string.IsNullOrWhiteSpace(job?.Path))
                GenerateParameterControls(job.Path);
        }

        private async void btnTunJob_Click(object sender, EventArgs e)
        {
            var job = cmbJobs.SelectedItem as JobDefinition;
            if (job == null || string.IsNullOrWhiteSpace(job.Path))
            {
                MessageBox.Show("Seleziona un job valido.");
                return;
            }

            var parametri = GetParameterValues(); // già definita
            EseguiJob(job.Path, parametri);
        }

        private Dictionary<string, string> GetParameterValues()
        {
            var result = new Dictionary<string, string>();

            foreach (Control ctrl in pnlParameters.Controls)
            {
                // Il controllo deve avere il Tag settato con il nome del parametro
                if (ctrl.Tag is not string name || string.IsNullOrWhiteSpace(name))
                    continue;

                string value = "";

                switch (ctrl)
                {
                    case TextBox tb:
                        value = tb.Text;
                        break;

                    case CheckBox cb:
                        value = cb.Checked.ToString().ToLower(); // true / false in formato CLI
                        break;

                    case FlowLayoutPanel panel: // per file/folder + bottone browse
                        var txt = panel.Controls.OfType<TextBox>().FirstOrDefault();
                        if (txt != null)
                            value = txt.Text;
                        break;
                }

                result[name] = value;
            }

            return result;
        }





        private async void EseguiJob(string jobPath, Dictionary<string, string> parametri)
        {
            txtLog.Clear();

            string pythonExe = Path.Combine("etl_venv", "Scripts", "python.exe");
            string runScript = Path.Combine(jobPath, "run.py");

            if (!File.Exists(pythonExe))
            {
                MessageBox.Show("Python non trovato (venv mancante).");
                return;
            }

            if (!File.Exists(runScript))
            {
                MessageBox.Show("Script run.py non trovato.");
                return;
            }

            // Costruisci gli argomenti
            string args = string.Join(" ", parametri.Select(kv =>
                $"--{kv.Key}=\"{kv.Value.Replace("\"", "\\\"")}\""));

            var psi = new ProcessStartInfo
            {
                FileName = pythonExe,
                Arguments = $"\"{runScript}\" {args}",
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };

            // Aggiungi PYTHONPATH per far trovare pyflowetl
            string frameworkPath = Path.GetFullPath("etl_framework");
            if (psi.EnvironmentVariables.ContainsKey("PYTHONPATH"))
                psi.EnvironmentVariables["PYTHONPATH"] = frameworkPath + ";" + psi.EnvironmentVariables["PYTHONPATH"];
            else
                psi.EnvironmentVariables["PYTHONPATH"] = frameworkPath;

            var process = new Process { StartInfo = psi };
            process.OutputDataReceived += (s, e) => LogManager.Append(e.Data);
            process.ErrorDataReceived += (s, e) => LogManager.Append(e.Data, isError: true);

            process.Start();
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();
            await process.WaitForExitAsync();
        }

    }
}

