import unittest
from unittest.mock import MagicMock, patch

import test_core


class DemoPlayerTests(unittest.TestCase):
    setUp = test_core.WebTests.setUp
    auth = test_core.WebTests.auth

    def test_player_and_video_require_login(self):
        for url in ("/demo/", "/demo/heatwave-audit-oci-streaming-demo.mp4"):
            self.assertEqual(self.client.get(url).status_code, 302)

    def test_signed_in_player_uses_external_assets_and_has_no_mp4_download_link(self):
        self.auth()
        with patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()):
            response = self.client.get("/demo/")
        self.assertEqual(response.status_code, 200)
        text = response.get_data(as_text=True)
        self.assertIn("controls playsinline", text)
        self.assertIn('src="player.js"', text)
        self.assertIn('href="player.css"', text)
        self.assertNotIn("<style>", text)
        self.assertNotIn("<script>", text)
        self.assertNotIn("Download MP4", text)
        self.assertNotIn("attachment", response.headers.get("Content-Disposition", ""))
        self.assertIn("script-src 'self'", response.headers["Content-Security-Policy"])
        response.close()

    def test_video_supports_byte_ranges_for_browser_playback_and_seeking(self):
        self.auth()
        with patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()):
            response = self.client.get(
                "/demo/heatwave-audit-oci-streaming-demo.mp4", headers={"Range": "bytes=0-63"}
            )
        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.mimetype, "video/mp4")
        self.assertEqual(len(response.data), 64)
        self.assertTrue(response.headers["Content-Range"].startswith("bytes 0-63/"))
        self.assertTrue(response.headers["Content-Disposition"].startswith("inline;"))
        response.close()

    def test_only_playback_and_companion_assets_are_served(self):
        self.auth()
        with patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()):
            for url in ("/demo/source/preview_app.py", "/demo/../app.py", "/demo/storyboard.json"):
                self.assertEqual(self.client.get(url).status_code, 404)
            for name, content_type in [
                ("player.css", "text/css"),
                ("player.js", "text/javascript"),
                ("captions.vtt", "text/vtt"),
            ]:
                response = self.client.get("/demo/" + name)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.mimetype, content_type)
                response.close()
