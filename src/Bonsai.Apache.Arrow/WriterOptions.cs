using System;
using System.ComponentModel;
using System.Xml;
using System.Xml.Serialization;

namespace Bonsai.Apache.Arrow
{
    /// <summary>Settings for Arrow file writers.</summary>
    public abstract class WriterOptions
    {
        /// <summary>Gets or sets the output file path.</summary>
        [Description("The path of the output file.")]
        [Editor("Bonsai.Design.SaveFileNameEditor, Bonsai.Design", "System.Drawing.Design.UITypeEditor, System.Drawing, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a")]
        public string? FileName { get; set; }

        /// <summary>Gets or sets the suffix appended to the file name.</summary>
        [Description("The suffix appended to the output file name.")]
        public Bonsai.IO.PathSuffix Suffix { get; set; }

        /// <summary>Gets or sets whether an existing output file may be replaced.</summary>
        [DefaultValue(false)]
        [Description("Allow replacing an existing output file.")]
        public bool Overwrite { get; set; }

        /// <summary>Gets or sets the member paths selected as columns.</summary>
        [Description("The comma-separated member paths selected as columns.")]
        [Editor("Bonsai.IO.Design.DataMemberSelectorEditor, Bonsai.System.Design", "System.Drawing.Design.UITypeEditor, System.Drawing, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a")]
        public string? Selector { get; set; }

        /// <summary>Gets or sets the maximum rows per record batch.</summary>
        [DefaultValue(100_000)]
        [Description("The maximum rows per record batch.")]
        public int BatchSize { get; set; } = 100_000;

        /// <summary>Gets or sets whether writing runs on a background worker.</summary>
        [DefaultValue(true)]
        [Description("Queue writes on a background worker. Input objects must remain unchanged until written.")]
        public bool Buffered { get; set; } = true;

        /// <summary>Gets or sets the maximum pending background writes.</summary>
        [DefaultValue(1024)]
        [Description("The maximum pending background writes, excluding the item currently being processed.")]
        public int QueueCapacity { get; set; } = 1024;

        /// <summary>Gets or sets the flush interval; zero disables timed flushing.</summary>
        [XmlIgnore]
        [DefaultValue(typeof(TimeSpan), "00:00:05")]
        [Description("Background writing only: flush partial batches periodically. Zero disables timed flushing.")]
        public TimeSpan FlushInterval { get; set; } = TimeSpan.FromSeconds(5);

        /// <summary>Gets or sets the XML representation of the flush interval.</summary>
        [Browsable(false)]
        [XmlElement(nameof(FlushInterval))]
        public string FlushIntervalXml
        {
            get => XmlConvert.ToString(FlushInterval);
            set => FlushInterval = XmlConvert.ToTimeSpan(value);
        }
    }
}
