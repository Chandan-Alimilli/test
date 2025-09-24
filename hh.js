function renderCharts(wrapper, dataArray) {
  if (!wrapper || !Array.isArray(dataArray) || dataArray.length === 0) {
    console.warn("⚠️ Invalid wrapper or dataArray");
    return;
  }

  const sample = dataArray[0];
  const keys = Object.keys(sample);
  console.log("📦 Keys available:", keys);

  const tableRules = {
    ACCOUNTS: {
      flags: ["APPL_APRV_IN", "BK_IN"],
      categories: [],
      numerics: ["APRV_LOAN_AM", "APRV_LOAN_PYMT_AM", "APRV_PYMT_AM"],
    },
    AUTO_FNCE_ORGN_REFN_ELG: {
      flags: ["REFN_EL_IN", "STATE_ALOW_IN"],
      categories: ["STATE_CD"],
    },
    AUTO_FNCE_ORGN_REFN_CLSNG_FEE: {
      flags: [],
      categories: ["LIEN_HLDR_NM"],
      numerics: ["ORGN_LOAN_PYF_AM", "VHCL_FEE_AM", "ADDL_FEE_AM"],
    },
    AUTO_FNCE_ORGN_REFN_CLSE: {
      flags: ["STS_CD"],
      categories: ["CLSE_TASK_STG_STS_TX"],
    },
    AUTOBOOK: {
      categories: ["ORGN_CHNL_NM", "PROD_TYPE_NM"],
    },
  };

  const detectedTable = keys.includes("APPL_APRV_IN")
    ? "ACCOUNTS"
    : keys.includes("REFN_EL_IN")
    ? "AUTO_FNCE_ORGN_REFN_ELG"
    : keys.includes("LIEN_HLDR_NM")
    ? "AUTO_FNCE_ORGN_REFN_CLSNG_FEE"
    : keys.includes("CLSE_TASK_STG_STS_TX")
    ? "AUTO_FNCE_ORGN_REFN_CLSE"
    : keys.includes("ORGN_CHNL_NM")
    ? "AUTOBOOK"
    : null;

  if (!detectedTable) {
    console.warn("⚠️ No table detected for keys:", keys);
    return;
  }

  const config = tableRules[detectedTable] || {};
  const flags = config.flags || [];
  const categories = config.categories || [];
  const numerics = config.numerics || [];

  const dateKey = keys.find(
    (k) => k.toLowerCase().includes("date") || k.toLowerCase().endsWith("_dt")
  );

  let chartCount = 0;

  // ✅ Special bar chart: Approved vs Booked for "ACCOUNTS" table
  if (detectedTable === "ACCOUNTS") {
    const approvedCount = dataArray.filter((d) => {
      const value = d.APPL_APRV_IN;
      return value === 1 || value === true || value === "1" || value === "true";
    }).length;
    const bookedCount = dataArray.filter((d) => {
      const value = d.BK_IN;
      return value === 1 || value === true || value === "1" || value === "true";
    }).length;

    const approvalCanvas = document.createElement("canvas");
    approvalCanvas.className = "approval-bar-chart";
    approvalCanvas.style.maxWidth = "380px";
    approvalCanvas.style.maxHeight = "300px";
    approvalCanvas.style.marginTop = "12px";
    approvalCanvas.style.borderRadius = "10px";
    wrapper.appendChild(approvalCanvas);

    const ctx = approvalCanvas.getContext("2d");
    const gradientColors = [
      ctx.createLinearGradient(0, 0, approvalCanvas.width, 0),
      ctx.createLinearGradient(0, 0, approvalCanvas.width, 0),
    ];
    gradientColors[0].addColorStop(0, "#0775f3");
    gradientColors[0].addColorStop(1, "#02053b");
    gradientColors[1].addColorStop(0, "#ff7043");
    gradientColors[1].addColorStop(1, "#701d03");

    new Chart(approvalCanvas, {
      type: "bar",
      data: {
        labels: ["Approved", "Booked"],
        datasets: [
          {
            label: "Applications",
            data: [approvedCount, bookedCount],
            backgroundColor: gradientColors,
            borderRadius: 6,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          title: {
            display: true,
            text: "Approved vs Booked Applications",
            color: "#0775f3",
          },
          legend: { display: false },
        },
        scales: {
          x: { ticks: { color: "#0775f3" } },
          y: { beginAtZero: true, ticks: { color: "#0775f3" } },
        },
      },
    });
    chartCount++;
  }

  // ✅ Flag-based bar charts (1 vs 0 or true vs false)
  for (const key of flags) {
    const trueCount = dataArray.filter((d) => {
      const value = d[key];
      return value === 1 || value === true || value === "1" || value === "true";
    }).length;
    const falseCount = dataArray.filter((d) => {
      const value = d[key];
      return (
        value === 0 ||
        value === false ||
        value === "0" ||
        value === "false" ||
        value === null ||
        value === undefined
      );
    }).length;

    const canvas = document.createElement("canvas");
    canvas.className = "flag-bar-chart";
    canvas.style.maxWidth = "380px";
    canvas.style.maxHeight = "300px";
    canvas.style.marginTop = "12px";
    canvas.style.borderRadius = "10px";
    wrapper.appendChild(canvas);

    const ctx = canvas.getContext("2d");
    const gradientColors = [
      ctx.createLinearGradient(0, 0, canvas.width, 0),
      ctx.createLinearGradient(0, 0, canvas.width, 0),
    ];
    gradientColors[0].addColorStop(0, "#0775f3");
    gradientColors[0].addColorStop(1, "#02053b");
    gradientColors[1].addColorStop(0, "#ff7043");
    gradientColors[1].addColorStop(1, "#701d03");

    new Chart(canvas, {
      type: "bar",
      data: {
        labels: ["Yes (1/True)", "No (0/False)"],
        datasets: [
          {
            label: key,
            data: [trueCount, falseCount],
            backgroundColor: gradientColors,
            borderRadius: 6,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          title: {
            display: true,
            text: `${key} Distribution`,
            color: "#0775f3",
          },
          legend: { display: false },
        },
        scales: {
          x: { ticks: { color: "#0775f3" } },
          y: { beginAtZero: true, ticks: { color: "#0775f3" } },
        },
      },
    });
    chartCount++;
  }

  // ✅ Pie charts for categories
  for (const key of categories) {
    console.log(`📈 Processing category: ${key}`); // Log the category being processed
    const canvas = document.createElement("canvas");
    canvas.className = "pie-chart-canva";
    if (!document.body.classList.contains("light_mode")) {
      canvas.classList.add("dark-mode");
    }
    wrapper.appendChild(canvas);

    const values = dataArray
      .map((d) => d[key])
      .filter(
        (v) => v !== null && v !== undefined && v.toString().trim() !== ""
      );
    console.log(`📊 Values for ${key}:`, values); // Log filtered values

    if (values.length === 0) {
      console.warn(`⚠️ No valid data for ${key}, skipping pie chart`);
      wrapper.removeChild(canvas);
      continue;
    }

    const counts = values.reduce((acc, v) => {
      const val = v?.toString().trim() || "Unknown";
      acc[val] = (acc[val] || 0) + 1;
      return acc;
    }, {});
    console.log(`📊 Counts for ${key}:`, counts); // Log the counts

    const ctx = canvas.getContext("2d");
    const gradientColors = [];
    const data = Object.values(counts);
    const labels = Object.keys(counts);

    if (data.length === 0) {
      console.warn(`⚠️ No data to display for ${key} pie chart`);
      wrapper.removeChild(canvas);
      continue;
    }

    const gradientPairs = [
      { start: "#88b8ff", end: "#0775f3" },
      { start: "#0775f3", end: "#02053b" },
      { start: "#26a69a", end: "#0288d1" },
      { start: "#ff7043", end: "#701d03" },
      { start: "#ab47bc", end: "#02053b" },
      { start: "#ff7043", end: "#701d03" },
      { start: "#b59c52", end: "#f0b505" },
      { start: "#03a9f4", end: "#0288d1" },
      { start: "#ffca28", end: "#ab47bc" },
      { start: "#ab47bc", end: "#8e24aa" },
    ];

    for (let i = 0; i < data.length; i++) {
      const pairIndex = i % gradientPairs.length;
      const gradient = ctx.createLinearGradient(0, 0, canvas.width, 0);
      gradient.addColorStop(0, gradientPairs[pairIndex].start);
      gradient.addColorStop(1, gradientPairs[pairIndex].end);
      gradientColors.push(gradient);
    }

    try {
      new Chart(canvas, {
        type: "pie",
        data: {
          labels: labels,
          datasets: [
            {
              data: data,
              backgroundColor: gradientColors,
            },
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: true,
          plugins: {
            title: {
              display: true,
              text: `${key} Distribution`,
              color: "#0288d1",
            },
            legend: {
              position: "top",
              labels: { color: "#0288d1" },
            },
          },
        },
      });
      chartCount++;
      console.log(`✅ Pie chart rendered for ${key}`);
    } catch (error) {
      console.error(`❌ Error rendering pie chart for ${key}:`, error);
      wrapper.removeChild(canvas);
    }
  }

  // ✅ Numeric charts (bar + line) over date
  if (numerics.length > 0 && dateKey) {
    const sorted = [...dataArray]
      .filter((d) => d[dateKey] && !isNaN(new Date(d[dateKey]).getTime()))
      .sort((a, b) => new Date(a[dateKey]) - new Date(b[dateKey]));
    if (sorted.length === 0) return;

    const barCanvas = document.createElement("canvas");
    barCanvas.className = "bar-chart-canvas";
    barCanvas.style.height = "180px";
    barCanvas.style.borderRadius = "12px";
    barCanvas.style.padding = "12px";
    wrapper.appendChild(barCanvas);

    new Chart(barCanvas, {
      type: "bar",
      data: {
        labels: sorted.map((d) => new Date(d[dateKey]).toLocaleDateString()),
        datasets: numerics.map((num, index) => ({
          label: num,
          data: sorted.map((d) => parseFloat(d[num] || 0)),
          backgroundColor: ["#0775f3", "#ff7043", "#26a69a"][index % 3],
          borderColor: ["#0775f3", "#ff7043", "#26a69a"][index % 3],
          borderWidth: 1,
        })),
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          title: {
            display: true,
            text: `Trends: ${numerics.join(", ")}`,
            color: "#0775f3",
          },
          legend: {
            labels: { color: "#0775f3" },
          },
        },
        scales: {
          x: {
            title: { display: true, text: dateKey, color: "#0775f3" },
            ticks: { color: "#0775f3" },
          },
          y: {
            beginAtZero: true,
            title: { display: true, text: "Amount", color: "#0775f3" },
            ticks: { color: "#0775f3" },
          },
        },
      },
    });
    chartCount++;

    const lineCanvas = document.createElement("canvas");
    lineCanvas.className = "line-chart-canvas";
    lineCanvas.style.height = "180px";
    lineCanvas.style.borderRadius = "12px";
    lineCanvas.style.padding = "12px";
    wrapper.appendChild(lineCanvas);

    new Chart(lineCanvas, {
      type: "line",
      data: {
        labels: sorted.map((d) => new Date(d[dateKey]).toLocaleDateString()),
        datasets: numerics.map((num, index) => ({
          label: num,
          data: sorted.map((d) => parseFloat(d[num] || 0)),
          borderColor: ["#0775f3", "#ff7043", "#26a69a"][index % 3],
          borderWidth: 2,
          fill: false,
        })),
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          title: {
            display: true,
            text: `Trends: ${numerics.join(", ")}`,
            color: "#0775f3",
          },
          legend: {
            labels: { color: "#0775f3" },
          },
        },
        scales: {
          x: {
            title: { display: true, text: dateKey, color: "#0775f3" },
            ticks: { color: "#0775f3" },
          },
          y: {
            beginAtZero: true,
            title: { display: true, text: "Amount", color: "#0775f3" },
            ticks: { color: "#0775f3" },
          },
        },
      },
    });
    chartCount++;
  }

  console.log(`📊 Total charts rendered: ${chartCount}`);
}
