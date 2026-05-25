using System;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Drawing;
using System.Windows.Forms;
using Bonsai.Design;
using ParquetSharp;

namespace Bonsai.Parquet.Design
{
    /// <summary>
    /// Provides a design-time collection editor for editing a Parquet schema
    /// expressed as a collection of <see cref="ColumnDefinition"/> entries.
    /// </summary>
    /// <remarks>
    /// In addition to the standard <see cref="DescriptiveCollectionEditor"/>
    /// behavior, this editor adds a "Load Schema" button that lets the user
    /// pick an existing <c>.parquet</c> file and populate the collection from
    /// its inferred schema via
    /// <see cref="ConverterHelper.InferColumns(SchemaDescriptor)"/>.
    /// </remarks>
    public class ParquetSchemaEditor : DescriptiveCollectionEditor
    {
        bool _shouldReload;
        ColumnDefinition[] _loadedSchema;

        /// <summary>
        /// Initializes a new instance of the <see cref="ParquetSchemaEditor"/>
        /// class bound to a <see cref="Collection{T}"/> of
        /// <see cref="ColumnDefinition"/>.
        /// </summary>
        public ParquetSchemaEditor() : base(typeof(Collection<ColumnDefinition>)) { }

        /// <inheritdoc/>
        public override object EditValue(ITypeDescriptorContext context, IServiceProvider provider, object value)
        {
            do
            {
                _shouldReload = false;
                value = base.EditValue(context, provider, value);
                if (_shouldReload)
                    value = new Collection<ColumnDefinition>(_loadedSchema);
            }
            while (_shouldReload);
            return value;
        }

        protected override CollectionForm CreateCollectionForm()
        {
            var form = base.CreateCollectionForm();
            form.Text = "Edit Parquet Schema";
            form.Shown += OnFormShown;
            return form;
        }

        void OnFormShown(object sender, EventArgs e)
        {
            var form = (Form)sender;
            var okBtn = FindButton(form, "OK");
            if (okBtn == null) return;

            var loadBtn = new Button
            {
                Text = "Load Schema",
                Size = new Size(okBtn.Width + 20, okBtn.Height),
                Margin = okBtn.Margin,
            };
            loadBtn.Click += (_, __) => OnLoadSchema(form);

            var panel = okBtn.Parent;
            if (panel is TableLayoutPanel tbl)
            {
                int cols = tbl.ColumnCount;
                tbl.ColumnCount = cols + 1;
                tbl.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
                for (int i = cols - 1; i >= 0; i--)
                {
                    var ctrl = tbl.GetControlFromPosition(i, 0);
                    if (ctrl != null) tbl.SetColumn(ctrl, i + 1);
                }
                tbl.Controls.Add(loadBtn, 0, 0);
            }
            else
            {
                panel?.Controls.Add(loadBtn);
            }
        }

        void OnLoadSchema(Form form)
        {
            using (var dlg = new OpenFileDialog
            {
                Title = "Select Parquet File",
                Filter = "Parquet files (*.parquet)|*.parquet|All files (*.*)|*.*",
            })
            {
                if (dlg.ShowDialog(form) != DialogResult.OK) return;

                try
                {
                    using (var reader = new ParquetFileReader(dlg.FileName))
                    {
                        _loadedSchema = ConverterHelper.InferColumns(reader.FileMetaData.Schema);
                        _shouldReload = true;
                        form.Close();
                    }
                }
                catch (Exception ex)
                {
                    MessageBox.Show(form, ex.Message, "Load Schema", MessageBoxButtons.OK, MessageBoxIcon.Error);
                }
            }
        }

        static Button FindButton(Control root, string text)
        {
            foreach (Control c in root.Controls)
            {
                if (c is Button b && b.Text == text) return b;
                var found = FindButton(c, text);
                if (found != null) return found;
            }
            return null;
        }
    }
}
