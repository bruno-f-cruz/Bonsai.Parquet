using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Drawing;
using System.Drawing.Design;
using System.Linq;
using System.Numerics;
using System.Windows.Forms;
using Bonsai.ImGui.Design;
using Hexa.NET.ImGui;
using Gui = Hexa.NET.ImGui.ImGui;


#if BONSAI_ARROW
using ColumnDefinition = Bonsai.Apache.Arrow.ColumnDefinition;
using ColumnType = Bonsai.Apache.Arrow.ArrowColumnType;
namespace Bonsai.Apache.Arrow.Design
#else
using ColumnDefinition = Bonsai.Apache.Parquet.ColumnDefinition;
using ColumnType = Bonsai.Apache.Parquet.ParquetColumnType;
namespace Bonsai.Apache.Parquet.Design
#endif
{
    /// <summary>Provides the shared Dear ImGui schema editing interface.</summary>
    public abstract class SchemaEditor : UITypeEditor
    {
        /// <summary>Gets the displayed format name.</summary>
        protected abstract string FormatName { get; }
        /// <summary>Gets the file dialog filter.</summary>
        protected abstract string FileFilter { get; }
        /// <summary>Loads columns from a file.</summary>
        protected abstract ColumnDefinition[] LoadColumns(string path);

        /// <inheritdoc/>
        public override UITypeEditorEditStyle GetEditStyle(ITypeDescriptorContext context) => UITypeEditorEditStyle.Modal;

        /// <inheritdoc/>
        public override object EditValue(ITypeDescriptorContext context, IServiceProvider provider, object value)
        {
            var original = value as Collection<ColumnDefinition>;
            var columns = original?.Select(Clone).ToList() ?? new List<ColumnDefinition>();
            var names = Enum.GetNames(typeof(ColumnType));
            var error = string.Empty;
            var selected = -1;
            var loadingSchema = false;
            using var form = new Form { Text = $"Edit {FormatName} Schema", ClientSize = new Size(900, 520), StartPosition = FormStartPosition.CenterParent };
            using var control = new ImGuiControl { Dock = DockStyle.Fill };
            using var timer = new Timer { Interval = 16 };
            form.Controls.Add(control);
            timer.Tick += (_, __) => control.Invalidate();
            form.Shown += (_, __) => timer.Start();
            control.Error += (_, args) => error = args.Message;
            control.Render += (_, __) =>
            {
                Gui.SetNextWindowPos(Vector2.Zero);
                Gui.SetNextWindowSize(Gui.GetIO().DisplaySize);
                Gui.Begin("Schema", ImGuiWindowFlags.NoDecoration | ImGuiWindowFlags.NoMove | ImGuiWindowFlags.NoSavedSettings);
                try
                {
                    if (Gui.Button("Load Schema") && !loadingSchema)
                    {
                        loadingSchema = true;
                        // A modal dialog pumps window messages. Open it only after this
                        // paint (including ImGui.Render and SwapBuffers) has finished.
                        form.BeginInvoke(new Action(() =>
                        {
                            if (form.IsDisposed || control.IsDisposed) return;
                            timer.Stop();
                            try
                            {
                                using var dialog = new OpenFileDialog { Title = $"Select {FormatName} File", Filter = FileFilter };
                                if (dialog.ShowDialog(form) == DialogResult.OK)
                                {
                                    columns = LoadColumns(dialog.FileName).ToList();
                                    selected = -1;
                                    error = string.Empty;
                                }
                            }
                            catch (Exception ex) { error = ex.Message; }
                            finally
                            {
                                loadingSchema = false;
                                if (!form.IsDisposed && !control.IsDisposed)
                                {
                                    timer.Start();
                                    control.Invalidate();
                                }
                            }
                        }));
                    }
                    Gui.SameLine();
                    if (Gui.Button("Add Column")) { columns.Add(new ColumnDefinition()); selected = columns.Count - 1; }
                    Gui.SameLine();
                    if (Gui.Button("Remove") && selected >= 0 && selected < columns.Count) { columns.RemoveAt(selected); selected = Math.Min(selected, columns.Count - 1); }
                    Gui.SameLine();
                    if (Gui.Button("Move Up") && selected > 0) { var item = columns[selected]; columns.RemoveAt(selected); columns.Insert(--selected, item); }
                    Gui.SameLine();
                    if (Gui.Button("Move Down") && selected >= 0 && selected < columns.Count - 1) { var item = columns[selected]; columns.RemoveAt(selected); columns.Insert(++selected, item); }
                    if (Gui.BeginTable("Columns", 5, ImGuiTableFlags.Borders | ImGuiTableFlags.RowBg | ImGuiTableFlags.ScrollY, new Vector2(0, -85)))
                    {
                        try
                        {
                            Gui.TableSetupColumn("Row"); Gui.TableSetupColumn("Name"); Gui.TableSetupColumn("Type");
                            Gui.TableSetupColumn("Nullable"); Gui.TableSetupColumn("Array"); Gui.TableHeadersRow();
                            for (int i = 0; i < columns.Count; i++)
                            {
                                var column = columns[i];
                                Gui.PushID(i);
                                Gui.TableNextRow(); Gui.TableSetColumnIndex(0);
                                if (Gui.Selectable((i + 1).ToString(), selected == i)) selected = i;
                                Gui.TableSetColumnIndex(1);
                                var name = column.Name;
                                if (Gui.InputText("##Name", ref name, 1024)) column.Name = name;
                                Gui.TableSetColumnIndex(2);
                                if (Gui.BeginCombo("##Type", column.Type.ToString()))
                                {
                                    foreach (var type in names)
                                        if (Gui.Selectable(type, type == column.Type.ToString())) column.Type = (ColumnType)Enum.Parse(typeof(ColumnType), type);
                                    Gui.EndCombo();
                                }
                                Gui.TableSetColumnIndex(3);
                                var nullable = column.IsNullable;
                                if (Gui.Checkbox("##Nullable", ref nullable)) column.IsNullable = nullable;
                                Gui.TableSetColumnIndex(4);
                                var array = column.IsArray;
                                if (Gui.Checkbox("##Array", ref array)) column.IsArray = array;
                                Gui.PopID();
                            }
                        }
                        finally { Gui.EndTable(); }
                    }
                    if (!string.IsNullOrEmpty(error))
                    {
                        Gui.PushTextWrapPos(0);
                        Gui.TextUnformatted(error);
                        Gui.PopTextWrapPos();
                    }
                    if (Gui.Button("OK"))
                    {
                        if (columns.Count == 0 || columns.Any(c => string.IsNullOrWhiteSpace(c.Name))) error = "Each schema must contain named columns.";
                        else if (columns.Select(c => c.Name).Distinct(StringComparer.Ordinal).Count() != columns.Count) error = "Column names must be unique.";
                        else form.BeginInvoke(new Action(() => { form.DialogResult = DialogResult.OK; form.Close(); }));
                    }
                    Gui.SameLine();
                    if (Gui.Button("Cancel")) form.BeginInvoke(new Action(() => { form.DialogResult = DialogResult.Cancel; form.Close(); }));
                }
                finally { Gui.End(); }
            };
            var result = form.ShowDialog();
            timer.Stop();
            return result == DialogResult.OK ? new Collection<ColumnDefinition>(columns) : value;
        }

        static ColumnDefinition Clone(ColumnDefinition column) => new ColumnDefinition
        { Name = column.Name, Type = column.Type, IsNullable = column.IsNullable, IsArray = column.IsArray };
    }
}
