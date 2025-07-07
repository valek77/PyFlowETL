namespace FlowGui
{
    partial class MainForm
    {
        /// <summary>
        ///  Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        ///  Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        ///  Required method for Designer support - do not modify
        ///  the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            cmbJobs = new ComboBox();
            label1 = new Label();
            pnlParameters = new FlowLayoutPanel();
            label2 = new Label();
            txtDescription = new TextBox();
            label3 = new Label();
            SuspendLayout();
            // 
            // cmbJobs
            // 
            cmbJobs.FormattingEnabled = true;
            cmbJobs.Location = new Point(22, 20);
            cmbJobs.Name = "cmbJobs";
            cmbJobs.Size = new Size(575, 23);
            cmbJobs.TabIndex = 0;
            cmbJobs.SelectedIndexChanged += cmbJobs_SelectedIndexChanged;
            cmbJobs.SelectedValueChanged += cmbJobs_SelectedValueChanged;
            // 
            // label1
            // 
            label1.AutoSize = true;
            label1.Location = new Point(22, 2);
            label1.Name = "label1";
            label1.Size = new Size(25, 15);
            label1.TabIndex = 1;
            label1.Text = "Job";
            // 
            // pnlParameters
            // 
            pnlParameters.AutoScroll = true;
            pnlParameters.BorderStyle = BorderStyle.FixedSingle;
            pnlParameters.FlowDirection = FlowDirection.TopDown;
            pnlParameters.Location = new Point(22, 186);
            pnlParameters.Name = "pnlParameters";
            pnlParameters.Size = new Size(1056, 297);
            pnlParameters.TabIndex = 2;
            pnlParameters.WrapContents = false;
            // 
            // label2
            // 
            label2.AutoSize = true;
            label2.Location = new Point(22, 168);
            label2.Name = "label2";
            label2.Size = new Size(58, 15);
            label2.TabIndex = 3;
            label2.Text = "Parametri";
            // 
            // txtDescription
            // 
            txtDescription.Location = new Point(22, 80);
            txtDescription.Multiline = true;
            txtDescription.Name = "txtDescription";
            txtDescription.ReadOnly = true;
            txtDescription.Size = new Size(575, 61);
            txtDescription.TabIndex = 4;
            // 
            // label3
            // 
            label3.AutoSize = true;
            label3.Location = new Point(27, 57);
            label3.Name = "label3";
            label3.Size = new Size(88, 15);
            label3.TabIndex = 5;
            label3.Text = "Descrizione Job";
            // 
            // MainForm
            // 
            AutoScaleDimensions = new SizeF(7F, 15F);
            AutoScaleMode = AutoScaleMode.Font;
            ClientSize = new Size(1194, 842);
            Controls.Add(label3);
            Controls.Add(txtDescription);
            Controls.Add(label2);
            Controls.Add(pnlParameters);
            Controls.Add(label1);
            Controls.Add(cmbJobs);
            Name = "MainForm";
            Text = "FlowGui";
            Load += MainForm_Load;
            ResumeLayout(false);
            PerformLayout();
        }

        #endregion

        private ComboBox cmbJobs;
        private Label label1;
        private FlowLayoutPanel pnlParameters;
        private Label label2;
        private TextBox txtDescription;
        private Label label3;
    }
}
