class ButterflyDl < Formula
  desc "High-performance OpenStreetMap data downloader with intelligent source routing"
  homepage "https://github.com/butterfly-osm/butterfly-dl"
  url "https://github.com/butterfly-osm/butterfly-dl/releases/download/vVERSION/butterfly-dl-vVERSION-x86_64-apple-darwin.tar.gz"
  sha256 "SHA256_PLACEHOLDER"
  license "MIT"
  version "VERSION"

  depends_on "rust" => :build

  def install
    bin.install "butterfly-dl"
    doc.install "README.md"
    doc.install "LICENSE"
  end

  test do
    assert_match "butterfly-dl", shell_output("#{bin}/butterfly-dl --help")
  end
end