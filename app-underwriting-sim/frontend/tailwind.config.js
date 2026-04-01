/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        "databricks-red": "#FF3621",
        "databricks-dark": "#1B3139",
        "databricks-gray": "#454C52",
        "databricks-light": "#F5F7F9",
      },
    },
  },
  plugins: [],
};
