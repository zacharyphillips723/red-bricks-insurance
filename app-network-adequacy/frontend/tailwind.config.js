/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        databricks: {
          red: "#FF3621",
          dark: "#1B3139",
          gray: "#454C52",
          light: "#F5F7F9",
        },
      },
    },
  },
  plugins: [],
};
