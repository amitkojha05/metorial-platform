export let parseRedisUrl = (url: string) => {
  let u = new URL(url);
  let db = u.pathname.slice(1);

  return {
    host: u.hostname,
    port: u.port ? Number.parseInt(u.port, 10) : 6379,
    username: u.username || undefined,
    password: decodeURIComponent(u.password),
    db: db ? Number.parseInt(db, 10) : 0,
    tls: u.protocol === 'rediss:' ? { rejectUnauthorized: false } : undefined
  };
};
