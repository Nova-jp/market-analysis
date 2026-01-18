import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: 'export', // 静的エクスポートを有効化
  images: {
    unoptimized: true,
  },
  eslint: {
    ignoreDuringBuilds: true, // ビルド時のESLintチェックをスキップ
  },
  typescript: {
    ignoreBuildErrors: true, // ビルド時の型エラーをスキップ
  },
  // 開発時のリライト設定
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: "http://127.0.0.1:8000/api/:path*",
      },
    ];
  },
};

export default nextConfig;
