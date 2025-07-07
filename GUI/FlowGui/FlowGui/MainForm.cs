using FlowGui.App;
using System.IO;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;


namespace FlowGui
{
    public partial class MainForm : Form
    {
        public MainForm()
        {
            InitializeComponent();
        }

        private void MainForm_Load(object sender, EventArgs e)
        {
            cmbJobs.Items.Clear();

            cmbJobs.Items.Add(new JobDefinition
            {
                Name = "",         // oppure "-- Seleziona un Job --"
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
    }
}

