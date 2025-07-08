using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System;
using System.Windows.Forms;


namespace FlowGui.App
{
    public static class LogManager
    {
        private static TextBox? _outputBox;

        public static void SetOutput(TextBox txtBox)
        {
            _outputBox = txtBox;
        }

        public static void Append(string? text, bool isError = false)
        {
            if (string.IsNullOrWhiteSpace(text) || _outputBox == null)
                return;

            if (_outputBox.InvokeRequired)
            {
                _outputBox.Invoke(new Action(() => AppendInternal(text, isError)));
            }
            else
            {
                AppendInternal(text, isError);
            }
        }

        private static void AppendInternal(string text, bool isError)
        {
            if (isError)
                _outputBox.AppendText("[ERR] " + text + Environment.NewLine);
            else
                _outputBox.AppendText(text + Environment.NewLine);
        }

        public static void Clear()
        {
            _outputBox?.Clear();
        }
    }

}
