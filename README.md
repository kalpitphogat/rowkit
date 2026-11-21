# rowkit

The image builds `src/`, which is Team 67's Express Lane pipeline
(github.com/jason-scheffel/express-lane, commit c7d4aa4), used under the MIT
License. Team 67's copyright notice is kept in `LICENSE`. It needs AVX-512
(built for Cascade Lake).

`legacy/` holds our own earlier pipeline (AVX2, scored 0.98 s). It is kept
for reference and is not part of the image.
