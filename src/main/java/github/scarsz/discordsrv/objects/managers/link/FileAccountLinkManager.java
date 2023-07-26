/*
 * DiscordSRV - https://github.com/DiscordSRV/DiscordSRV
 *
 * Copyright (C) 2016 - 2022 Austin "Scarsz" Shapiro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-3.0.html>.
 */

package github.scarsz.discordsrv.objects.managers.link;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.MalformedJsonException;
import github.scarsz.discordsrv.Debug;
import github.scarsz.discordsrv.DiscordSRV;
import github.scarsz.discordsrv.util.DiscordUtil;
import github.scarsz.discordsrv.util.Either;
import github.scarsz.discordsrv.util.LangUtil;
import github.scarsz.discordsrv.util.MessageUtil;
import github.scarsz.discordsrv.util.PrettyUtil;
import net.dv8tion.jda.api.entities.User;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

public class FileAccountLinkManager extends AbstractAccountLinkManager {

    private final DualHashBidiMap<String, Either<UUID, JBUser>> linkedAccounts = new DualHashBidiMap<>();

    @SuppressWarnings("ConstantConditions") // MalformedJsonException is a checked exception
    public FileAccountLinkManager() {
        if (!DiscordSRV.getPlugin().getLinkedAccountsFile().exists()
                || DiscordSRV.getPlugin().getLinkedAccountsFile().length() == 0)
            return;
        linkedAccounts.clear();

        try {
            String fileContent = FileUtils.readFileToString(DiscordSRV.getPlugin().getLinkedAccountsFile(),
                    StandardCharsets.UTF_8);
            if (fileContent == null || StringUtils.isBlank(fileContent))
                fileContent = "{}";
            JsonObject jsonObject;
            try {
                jsonObject = DiscordSRV.getPlugin().getGson().fromJson(fileContent, JsonObject.class);
            } catch (Throwable t) {
                if (!(t instanceof MalformedJsonException) && !(t instanceof JsonSyntaxException)
                        || !t.getMessage().contains("JsonPrimitive")) {
                    DiscordSRV.error("Failed to load linkedaccounts.json", t);
                    return;
                } else {
                    jsonObject = new JsonObject();
                }
            }

            jsonObject.entrySet().forEach(entry -> {
                String key = entry.getKey();
                if (entry.getValue().isJsonArray()) {
                    JsonArray value = entry.getValue().getAsJsonArray();
                    if (value.size() <= 0) {
                        return;
                    } else if (value.size() >= 2) {
                        JBUser user = new JBUser(UUID.fromString(value.get(0).getAsString()),
                                UUID.fromString(value.get(1).getAsString()));
                        linkedAccounts.put(key, Either.right(user));
                    }

                } else {
                    String value = entry.getValue().getAsString();
                    if (key.isEmpty() || value.isEmpty()) {
                        // empty values are not allowed.
                        return;
                    }

                    try {
                        linkedAccounts.put(key, Either.left(UUID.fromString(value)));
                    } catch (Exception e) {
                        try {
                            linkedAccounts.put(value, Either.left(UUID.fromString(key)));
                        } catch (Exception f) {
                            DiscordSRV.warning(
                                    "Failed to load linkedaccounts.json file. It's extremely recommended to delete your linkedaccounts.json file.");
                        }
                    }
                }
            });
        } catch (IOException e) {
            DiscordSRV.error("Failed to load linkedaccounts.json", e);
        }
    }

    @Override
    public boolean isInCache(UUID uuid) {
        // always in cache
        return true;
    }

    @Override
    public boolean isInCache(String discordId) {
        // always in cache
        return true;
    }

    @Override
    public Map<String, Either<UUID, JBUser>> getLinkedAccounts() {
        return linkedAccounts;
    }

    @Override
    public String getDiscordIdFromCache(UUID uuid) {
        return getDiscordId(uuid);
    }

    @Override
    public UUID getUuidFromCache(String discordId) {
        return getUuid(discordId);
    }

    @Override
    public Either<UUID, JBUser> getUuidsFromCache(String discordId) {
        return getUuids(discordId);
    }

    @Override
    public int getLinkedAccountCount() {
        return linkedAccounts.size();
    }

    @Override
    public String process(String linkCode, String discordId) {
        boolean contains;
        synchronized (linkedAccounts) {
            contains = linkedAccounts.containsKey(discordId);
        }

        User user = DiscordUtil.getUserById(discordId);
        String mention = user == null ? "" : user.getAsMention();

        if (contains) {
            if (DiscordSRV.config().getBoolean("MinecraftDiscordAccountLinkedAllowRelinkBySendingANewCode")) {
                unlink(discordId);
            } else {
                UUID[] uuidList = null;
                synchronized (linkedAccounts) {
                    uuidList = linkedAccounts.get(discordId).map(left -> new UUID[] { left },
                            right -> new UUID[] { right.javaID, right.bedrockID });
                }
                if (uuidList.length == 1) {
                    if ((uuidList[0].getMostSignificantBits() == 0) == (linkingCodes.get(linkCode)
                            .getMostSignificantBits() == 0)) {
                        OfflinePlayer offlinePlayer = DiscordSRV.getPlugin().getServer().getOfflinePlayer(uuidList[0]);
                        return LangUtil.Message.ALREADY_LINKED.toString()
                                .replace("%username%", PrettyUtil.beautifyUsername(offlinePlayer, "<Unknown>", false))
                                .replace("%uuid%", uuidList[0].toString());
                    }
                } else if (uuidList.length == 2) {
                    OfflinePlayer offlinePlayer = DiscordSRV.getPlugin().getServer().getOfflinePlayer(uuidList[0]);
                    OfflinePlayer offlinePlayer2 = DiscordSRV.getPlugin().getServer().getOfflinePlayer(uuidList[1]);
                    return LangUtil.Message.ALREADY_LINKED_JAVA_BEDROCK.toString()
                            .replace("%java_username%", PrettyUtil.beautifyUsername(offlinePlayer, "<Unknown>", false))
                            .replace("%java_uuid%", uuidList[0].toString())
                            .replace("%bedrock_username%",
                                    PrettyUtil.beautifyUsername(offlinePlayer2, "<Unknown>", false))
                            .replace("%bedrock_uuid%", uuidList[1].toString());
                }
                OfflinePlayer offlinePlayer = DiscordSRV.getPlugin().getServer().getOfflinePlayer(uuid);
                return LangUtil.Message.ALREADY_LINKED.toString()
                        .replace("%username%", PrettyUtil.beautifyUsername(offlinePlayer, "<Unknown>", false))
                        .replace("%uuid%", uuid.toString())
                        .replace("%mention%", mention);
            }
        }

        // strip the code to get rid of non-numeric characters
        linkCode = linkCode.replaceAll("[^0-9]", "");

        if (linkingCodes.containsKey(linkCode)) {
            link(discordId, linkingCodes.get(linkCode));
            UUID uuid = linkingCodes.remove(linkCode);

            OfflinePlayer player = Bukkit.getOfflinePlayer(uuid);
            if (player.isOnline()) {
                MessageUtil.sendMessage(Bukkit.getPlayer(getUuid(discordId)),
                        LangUtil.Message.MINECRAFT_ACCOUNT_LINKED.toString()
                                .replace("%username%", user == null ? "" : user.getName())
                                .replace("%id%", user == null ? "" : user.getId()));
            }

            return LangUtil.Message.DISCORD_ACCOUNT_LINKED.toString()
                    .replace("%name%", PrettyUtil.beautifyUsername(player, "<Unknown>", false))
                    .replace("%displayname%", PrettyUtil.beautifyNickname(player, "<Unknown>", false))
                    .replace("%uuid%", getUuid(discordId).toString())
                    .replace("%mention%", mention);
        }

        String reply = linkCode.length() == 4
                ? LangUtil.Message.UNKNOWN_CODE.toString()
                : LangUtil.Message.INVALID_CODE.toString();
        return reply
                .replace("%code%", linkCode)
                .replace("%mention%", mention);
    }

    @Override
    public String getDiscordId(UUID uuid) {
        synchronized (linkedAccounts) {
            for (Entry<String, Either<UUID, JBUser>> entry : linkedAccounts.entrySet()) {
                if (entry.getValue().isLeft()) {
                    if (entry.getValue().left().get().equals(uuid)) {
                        return entry.getKey();
                    }
                } else if (entry.getValue().isRight()) {
                    JBUser user = entry.getValue().right().get();
                    if (user.javaID.equals(uuid) || user.bedrockID.equals(uuid)) {
                        return entry.getKey();
                    }
                }
            }
        }
        return null;
    }

    @Override
    public String getDiscordIdBypassCache(UUID uuid) {
        return getDiscordId(uuid);
    }

    @Override
    public Map<UUID, String> getManyDiscordIds(Set<UUID> uuids) {
        Map<UUID, String> results = new HashMap<>();
        for (UUID uuid : uuids) {
            String discordId;
            synchronized (linkedAccounts) {
                discordId = linkedAccounts.getKey(uuid);
            }
            if (discordId != null)
                results.put(uuid, discordId);
        }
        return results;
    }

    @Override
    public UUID getUuid(String discordId) {
        synchronized (linkedAccounts) {
            Either<UUID, JBUser> uuids = linkedAccounts.get(discordId);
            if (uuids == null)
                return null;
            else if (uuids.isLeft())
                return uuids.left().get();
            else if (uuids.isRight())
                return uuids.right().get().javaID;
            else
                return null;
        }
    }

    @Override
    public Either<UUID, JBUser> getUuids(String discordId) {
        synchronized (linkedAccounts) {
            return linkedAccounts.get(discordId);
        }
    }

    @Override
    public UUID getUuidBypassCache(String discordId) {
        return getUuid(discordId);
    }

    @Override
    public Either<UUID, JBUser> getUuidsBypassCache(String discordId) {
        return getUuids(discordId);
    }

    @Override
    public Map<String, Either<UUID, JBUser>> getManyUuids(Set<String> discordIds) {
        Map<String, Either<UUID, JBUser>> results = new HashMap<>();
        for (String discordId : discordIds) {
            Either<UUID, JBUser> data;
            synchronized (linkedAccounts) {
                data = linkedAccounts.get(discordId);
            }
            if (data != null)
                results.put(discordId, data);
        }
        return results;
    }

    @Override
    public void link(String discordId, UUID uuid) {
        if (discordId.trim().isEmpty()) {
            throw new IllegalArgumentException("Empty discord id's are not allowed");
        }
        DiscordSRV.debug(Debug.ACCOUNT_LINKING, "File backed link: " + discordId + ": " + uuid);

        // make sure the user isn't linked
        unlink(uuid);

        synchronized (linkedAccounts) {
            if (linkedAccounts.containsKey(discordId)) {
                Either<UUID, JBUser> data = linkedAccounts.get(discordId);
                if (data.isLeft() || data.isEmpty()) {
                    if ((data.left().get().getMostSignificantBits() == 0) != (uuid.getMostSignificantBits() == 0)) {
                        JBUser user;
                        if (uuid.getMostSignificantBits() == 0) {
                            linkedAccounts.put(discordId, Either.right(user = new JBUser(data.left().get(), uuid)));
                        } else {
                            linkedAccounts.put(discordId, Either.right(user = new JBUser(uuid, data.left().get())));
                        }
                        afterLink(discordId, user.javaID);
                        afterLink(discordId, user.bedrockID);
                    } else {
                        unlink(discordId);
                        linkedAccounts.put(discordId, Either.left(uuid));
                        afterLink(discordId, uuid);
                    }
                } else if (data.isRight()) {
                    JBUser user = data.right().get();
                    if (uuid.getMostSignificantBits() == 0) {
                        user.bedrockID = uuid;
                    } else {
                        user.javaID = uuid;
                    }
                    unlink(discordId);
                    linkedAccounts.put(discordId, Either.right(user));
                    afterLink(discordId, user.javaID);
                    afterLink(discordId, user.bedrockID);
                }
            } else {
                unlink(discordId);
                linkedAccounts.put(discordId, Either.left(uuid));
                afterLink(discordId, uuid);
            }
        }
    }

    @Override
    public void unlink(UUID uuid) {
        String discordId;
        synchronized (linkedAccounts) {
            discordId = linkedAccounts.getKey(uuid);
        }
        if (discordId == null)
            return;

        synchronized (linkedAccounts) {
            beforeUnlink(uuid, discordId);
            linkedAccounts.removeValue(uuid);
        }

        afterUnlink(uuid, discordId);
    }

    @Override
    public void unlink(String discordId) {
        Either<UUID, JBUser> data;
        synchronized (linkedAccounts) {
            data = linkedAccounts.get(discordId);
        }
        if (data == null || data.isEmpty())
            return;

        synchronized (linkedAccounts) {
            if (data.isLeft()) {
                beforeUnlink(data.left().get(), discordId);
                linkedAccounts.remove(discordId);
            } else if (data.isRight()) {
                JBUser user = data.right().get();
                beforeUnlink(user.javaID, discordId);
                beforeUnlink(user.bedrockID, discordId);
                linkedAccounts.remove(discordId);
            }
        }
        if (data.isLeft()) {
            afterUnlink(data.left().get(), discordId);
        } else if (data.isRight()) {
            JBUser user = data.right().get();
            afterUnlink(user.javaID, discordId);
            afterUnlink(user.bedrockID, discordId);
        }
    }

    @Override
    public void save() {
        long startTime = System.currentTimeMillis();

        try {
            JsonObject map = new JsonObject();
            synchronized (linkedAccounts) {
                linkedAccounts.forEach((discordId, uuid) -> map.add(discordId,
                        uuid.<JsonElement>map(left -> new JsonPrimitive(left.toString()), right -> {
                            JsonArray arr = new JsonArray();
                            arr.add(right.javaID.toString());
                            arr.add(right.bedrockID.toString());
                            return arr;
                        })));
            }
            FileUtils.writeStringToFile(DiscordSRV.getPlugin().getLinkedAccountsFile(), map.toString(),
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            DiscordSRV.error(LangUtil.InternalMessage.LINKED_ACCOUNTS_SAVE_FAILED + ": " + e.getMessage());
            return;
        }

        DiscordSRV.info(LangUtil.InternalMessage.LINKED_ACCOUNTS_SAVED.toString().replace("{ms}",
                String.valueOf(System.currentTimeMillis() - startTime)));
    }

}
