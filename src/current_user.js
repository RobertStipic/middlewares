import jwt from "jsonwebtoken";

export const currentUser = (req, res, next) => {
  let token = null;

  const authHeader = req.headers.authorization;
  if (authHeader?.startsWith("Bearer ")) {
    token = authHeader.slice(7);
  } else if (req.session?.jwt) {
    token = req.session.jwt;
  }
  if (!token) {
    return next();
  }

  try {
    const payload = jwt.verify(token, process.env.JWT_PRIVATE_KEY);
    req.currentUser = payload;
  } catch (err) {
    res.status(401).send("Invalid token");
  }
  next();
};
